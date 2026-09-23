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

//! SQL adapter to the existing authenticated catalog and relational row APIs.
const std = @import("std");
const http_server = @import("http_server.zig");
const catalog = @import("../sql/catalog.zig");
const ast = @import("../sql/ast.zig");
const system_catalog = @import("../system_catalog/domain.zig");
const operation = @import("operation.zig");
const helpers = @import("http_route_helpers.zig");
const wire = @import("antfly_metadata_openapi").types;
const db_types = @import("../storage/db/types.zig");

fn supportsRangeGuards(server: *const http_server.ApiHttpServer) bool {
    const reads = server.table_reads orelse return false;
    const writes = server.table_writes orelse return false;
    return reads.supports_sql_range_guards and writes.supports_sql_range_guards and
        reads.vtable.open_relational_statement != null and
        (writes.vtable.commit_transaction_with_id != null or writes.vtable.commit_transaction_with_id_with_cancellation != null);
}

pub const Adapter = struct {
    server: *http_server.ApiHttpServer,
    identity: *?http_server.AuthenticatedIdentity,
    context: operation.RequestContext,
    database: []const u8 = "default",
    namespace: []const u8 = "public",
    /// Pgwire retains the original durable owner scope independently of its
    /// mutable, freshly authorized lookup namespace. HTTP leaves this null.
    session_namespace: ?[]const u8 = null,
    inherit_session_database: bool = false,
    inherit_session_namespace: bool = false,
    revision: ?u64 = null,
    target: ?system_catalog.Target = null,
    outcome_transaction_id: ?[32]u8 = null,
    session_id: ?[]const u8 = null,
    result_session_id: ?[32]u8 = null,
    transaction_status: @import("../sql/session.zig").Status = .idle,
    active_transaction: ?[16]u8 = null,
    staged: ?*@import("transactions.zig").OwnedTransactionCommitRequest = null,
    range_reads: ?*@import("transactions.zig").OwnedTransactionCommitRequest = null,
    ranges_staged: bool = false,
    inserting: bool = false,
    prepared_bindings: ?[]const @import("sql_prepared.zig").Binding = null,
    collect_prepared_bindings: ?*std.ArrayListUnmanaged(@import("sql_prepared.zig").Binding) = null,

    pub fn execute(self: *Adapter, alloc: std.mem.Allocator, compiled: *const @import("../sql/compiler.zig").Compiled, parameters: []const std.json.Value, limits: @import("../sql/runtime.zig").Limits, guarded_backend: ?catalog.Backend) !@import("../sql/runtime.zig").Result {
        const session_api = @import("sql_session.zig");
        const sessions = @import("../sql/session.zig");
        const previous_database = self.database;
        const previous_namespace = self.namespace;
        var inherited: ?@import("transactions.zig").SessionRegistry.SqlState = null;
        defer {
            self.database = previous_database;
            self.namespace = previous_namespace;
            if (inherited) |*state| state.deinit(self.server.alloc);
        }
        if (self.session_id) |encoded| if (self.inherit_session_database or self.inherit_session_namespace) {
            const id = @import("distributed_txn.zig").parseTxnIdHex(encoded) catch return error.SqlTransactionNotActive;
            if (try self.server.txn_sessions.principalAccess(self.server.alloc, id, http_server.transactionPrincipal(self.identity.*)) != .allowed) return error.SqlTransactionNotActive;
            inherited = (try self.server.txn_sessions.getSqlState(self.server.alloc, id)) orelse return error.SqlTransactionNotActive;
            if (self.inherit_session_database) self.database = inherited.?.metadata.database;
            if (self.inherit_session_namespace) self.namespace = inherited.?.metadata.namespace;
        };
        var coordinator = session_api.Coordinator{ .server = self.server, .identity = self.identity, .context = self.context };
        var owner = session_api.Adapter{ .alloc = self.server.alloc, .registry = &self.server.txn_sessions, .node_id = self.server.localSessionNodeId(), .commit_context = &coordinator, .commit_fn = session_api.Coordinator.commit, .supports_range_guards = supportsRangeGuards(self.server) };
        var session = sessions.Session{ .owner = owner.owner(), .scope = .{ .principal = http_server.transactionPrincipal(self.identity.*) orelse "", .database = self.database, .namespace = self.session_namespace orelse self.namespace } };
        if (self.session_id) |encoded| {
            const id = @import("distributed_txn.zig").parseTxnIdHex(encoded) catch return error.SqlTransactionNotActive;
            session.attach(id) catch |err| {
                if (err == error.SessionLeaseLost) self.outcome_transaction_id = std.fmt.bytesToHex(id, .lower);
                return err;
            };
        }
        // A request only borrows the durable session. Dropping an HTTP request
        // must not abandon it; pgwire disconnect is the explicit owner release.
        defer {
            self.transaction_status = session.status() catch .failed;
            self.result_session_id = if (session.transaction_id) |id| std.fmt.bytesToHex(id, .lower) else null;
        }
        errdefer |err| if (err != error.SqlWriteCapacityUnavailable) session.statementFailed() catch {};
        if (compiled.statement == .set_constraints) {
            if (parameters.len != 0) return error.InvalidSqlParameters;
            const id = session.transaction_id orelse return error.SqlTransactionNotActive;
            var result = try @import("../sql/runtime.zig").Result.empty(alloc, "SET CONSTRAINTS");
            errdefer result.deinit();
            const timing = compiled.statement.set_constraints;
            try @import("sql_constraint_timing.zig").set(self.server, alloc, self.identity.*, self.context, session.scope, self.namespace, id, timing.names, timing.deferred);
            return result;
        }
        const control: ?sessions.Control = switch (compiled.statement) {
            .begin => |options| .{ .begin = options },
            .commit => .commit,
            .rollback => .rollback,
            .savepoint => |name| .{ .savepoint = name },
            .rollback_to_savepoint => |name| .{ .rollback_to = name },
            .release_savepoint => |name| .{ .release = name },
            else => null,
        };
        if (control) |command| {
            if (parameters.len != 0) return error.InvalidSqlParameters;
            const control_lease = if (session.transaction_id != null and command != .commit)
                self.server.txn_sessions.tryAcquireCommitExecution(session.transaction_id.?) orelse return error.SqlWriteCapacityUnavailable
            else
                null;
            defer if (control_lease) |held| held.release();
            const tag: []const u8 = switch (command) {
                .begin => "BEGIN",
                .commit => "COMMIT",
                .rollback, .rollback_to => "ROLLBACK",
                .savepoint => "SAVEPOINT",
                .release => "RELEASE",
            };
            // Allocate the acknowledgement before mutating durable state.
            var result = try @import("../sql/runtime.zig").Result.empty(alloc, tag);
            errdefer result.deinit();
            const already_failed = command == .commit and (try session.status()) == .failed;
            const outcome = try session.execute(command);
            if (outcome.commit) |commit| {
                self.outcome_transaction_id = std.fmt.bytesToHex(commit.reconciliation_id, .lower);
                if (commit.outcome == .unknown) return error.SqlTransactionOutcomeUnknown;
                if (commit.outcome == .aborted and !already_failed) return error.SqlWriteConflict;
                if (commit.outcome == .aborted) result.output.command_tag = "ROLLBACK";
                result.output.mutation_outcome = switch (commit.outcome) {
                    .committed => .committed,
                    .committed_pending => .committed_pending,
                    .committed_repair_required => .committed_repair_required,
                    .aborted, .unknown => null,
                };
            }
            return result;
        }
        const implicit_merge = compiled.statement == .merge and session.transaction_id == null;
        if (implicit_merge) {
            if (!supportsRangeGuards(self.server)) return error.SqlRangeTrackingRequired;
            _ = try session.execute(.{ .begin = .{} });
        }
        var implicit_decided = false;
        errdefer if (implicit_merge and !implicit_decided) {
            _ = session.execute(.rollback) catch session.deinit();
        };
        if (session.transaction_id != null) {
            const transaction = try session.statement(switch (compiled.statement) {
                .select, .explain => false,
                else => true,
            });
            if (compiled.statement == .merge and transaction.isolation == .read_committed) return error.SqlRangeTrackingRequired;
            // DDL cannot bypass the native transaction's atomicity boundary.
            if (@import("../sql/ddl_runtime.zig").accepts(compiled.statement)) return error.UnsupportedSqlExecution;
            const id = session.transaction_id.?;
            var result = statement: {
                const execution_lease = self.server.txn_sessions.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
                defer execution_lease.release();
                _ = try session.statement(switch (compiled.statement) {
                    .select, .explain => false,
                    else => true,
                });
                var staged = try self.server.txn_sessions.cloneSqlStaged(alloc, id);
                defer staged.deinit(alloc);
                var read_guards: @import("transactions.zig").OwnedTransactionCommitRequest = .{};
                defer read_guards.deinit(self.server.alloc);
                self.range_reads = if (transaction.isolation != .read_committed) &read_guards else null;
                self.ranges_staged = false;
                self.active_transaction = id;
                self.staged = &staged;
                self.inserting = switch (compiled.statement) {
                    .insert => |insert| insert.conflict == null,
                    .merge => |merge| for (merge.arms) |arm| {
                        if (arm.action == .insert) break true;
                    } else false,
                    else => false,
                };
                defer {
                    self.active_transaction = null;
                    self.staged = null;
                    self.range_reads = null;
                    self.ranges_staged = false;
                    self.inserting = false;
                }
                var active_backend = guarded_backend orelse self.backend();
                if (guarded_backend != null) {
                    active_backend.atomic_statement_read_set = self.range_reads != null;
                    active_backend.coordinated_point_reads = self.range_reads != null;
                    active_backend.coordinated_index_reads = self.range_reads != null and staged.tables.len == 0;
                }
                var output = try @import("../sql/runtime.zig").execute(alloc, active_backend, compiled, parameters, limits);
                errdefer output.deinit();
                if (!self.ranges_staged and read_guards.tables.len != 0) {
                    _ = (try self.server.txn_sessions.stage(self.server.alloc, id, &read_guards)) orelse return error.SqlTransactionNotActive;
                }
                output.output.mutation_outcome = null;
                break :statement output;
            };
            errdefer result.deinit();
            if (implicit_merge) {
                // The statement lease and borrowed read view are gone before
                // commit admission. A decided/unknown outcome is never retried.
                const decision = try session.execute(.commit);
                const commit = decision.commit orelse return error.InvalidSqlBackendResponse;
                implicit_decided = true;
                self.outcome_transaction_id = std.fmt.bytesToHex(commit.reconciliation_id, .lower);
                result.output.mutation_outcome = switch (commit.outcome) {
                    .committed => .committed,
                    .committed_pending => .committed_pending,
                    .committed_repair_required => .committed_repair_required,
                    .aborted => return error.SqlWriteConflict,
                    .unknown => return error.SqlTransactionOutcomeUnknown,
                };
            }
            return result;
        }
        return @import("../sql/runtime.zig").execute(alloc, guarded_backend orelse self.backend(), compiled, parameters, limits);
    }

    pub fn backend(self: *Adapter) catalog.Backend {
        return .{ .ptr = self, .predicate_only_mutations = true, .atomic_statement_read_set = self.active_transaction != null and self.range_reads != null, .coordinated_point_reads = self.active_transaction != null and self.range_reads != null, .coordinated_index_reads = self.active_transaction != null and self.range_reads != null and (self.staged == null or self.staged.?.tables.len == 0), .vtable = &.{ .resolve_conflict_owners = resolveConflictOwners, .generate_row_id = generateRowId, .resolve = resolve, .scan = scan, .open_scan = openScan, .open_statement = openStatement, .mutate = mutate, .mutate_prepared = mutatePrepared, .prepare_mutations = prepareMutations, .ddl = ddl, .checkpoint = checkpoint } };
    }

    fn generateRowId(ptr: *anyopaque, alloc: std.mem.Allocator) ![]const u8 {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        try self.context.ensureActive();
        return @import("../storage/row_identity.zig").generate(alloc, self.server.sharedApiIo() orelse return error.UnsupportedSqlExecution);
    }

    fn resolveConflictOwners(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, columns: []const []const u8, expressions: []const catalog.ConflictExpression, conditions: []const catalog.Condition, mutations: []const catalog.Mutation) ![]const catalog.ConflictOwner {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        try self.verify(alloc, table);
        const integrity = @import("relational_integrity_commit.zig");
        const predicates = try @import("../sql/conflict_predicate.zig").toNative(alloc, conditions);
        const keys = try @import("../sql/conflict_predicate.zig").expressionsToNative(alloc, expressions);
        var snapshot = (try self.server.source.adminSnapshot()) orelse return error.IntegrityCatalogUnavailable;
        defer self.server.source.freeAdminSnapshot(&snapshot);
        const writes = try @import("../sql/mutation_images.zig").writes(db_types.BatchWrite, alloc, mutations);
        const previous = if (self.staged) |staged| try staged.distributedTables(alloc) else &.{};
        const owners = try integrity.resolveConflictOwners(alloc, self.server.table_reads orelse return error.UnsupportedSqlExecution, snapshot.tables, snapshot.ranges, table.physical_name, table.schema_version, columns, keys, predicates, writes, previous, self.context);
        try self.verify(alloc, table);
        const result = try alloc.alloc(catalog.ConflictOwner, owners.len);
        for (owners, result) |*owner, *out| out.* = .{ .key = owner.key, .identity = owner.identity, .identities = owner.identities, .guard = owner };
        return result;
    }

    fn conflictGuards(alloc: std.mem.Allocator, mutations: []const catalog.Mutation) !?@import("transactions.zig").TableCommitRequest.ConflictGuards {
        const integrity = @import("relational_integrity_commit.zig");
        const Command = @import("../storage/db/relational_integrity_contract.zig").Command;
        var generation: ?[32]u8 = null;
        var commands: std.ArrayList(Command) = .empty;
        for (mutations) |mutation| if (mutation.conflict_guard) |proof| {
            const owner: *const integrity.ConflictOwner = @ptrCast(@alignCast(proof));
            if (generation) |previous| if (!std.mem.eql(u8, &previous, &owner.generation_set)) return error.PreparedGenerationChanged;
            generation = owner.generation_set;
            try commands.appendSlice(alloc, owner.guards);
        };
        return if (generation) |version| .{ .generation_set = version, .commands = commands.items } else null;
    }

    fn ddl(ptr: *anyopaque, alloc: std.mem.Allocator, input: catalog.Ddl) !catalog.DdlOutcome {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        return @import("sql_catalog.zig").execute(self.server, self.identity.*, self.context, self.database, self.namespace, alloc, input);
    }

    fn checkpoint(ptr: *anyopaque) !void {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        try self.context.ensureActive();
    }

    fn resolve(ptr: *anyopaque, alloc: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        const target: system_catalog.Target = .{ .database = name.database orelse self.database, .namespace = name.namespace orelse self.namespace, .table = name.table };
        try target.validate();
        const logical = try target.resourceNameAlloc(alloc);
        const permission: @import("../usermgr/mod.zig").PermissionType = switch (action) {
            .read => .read,
            .write, .read_write => .write,
            .admin => .admin,
        };
        if (self.identity.*) |identity| {
            if (!http_server.permissionsAllow(identity.permissions, .table, logical, permission)) return error.Forbidden;
            if (action == .read_write and !http_server.permissionsAllow(identity.permissions, .table, logical, .read)) return error.Forbidden;
        }
        // A narrow definition read pins schema and physical identity in one
        // catalog epoch; never enumerate/clone every table on a SQL request.
        if (!self.server.source.vtable.supports_query_definitions) return error.UnsupportedSqlExecution;
        const bytes = try self.server.source.systemCatalog(alloc, self.context, .{ .resolve_many = .{ .targets = &.{target}, .include_query_definitions = true, .expected_revision = self.revision } });
        const snapshot = try std.json.parseFromSliceLeaky(system_catalog.ResolvedMany, alloc, bytes, .{ .allocate = .alloc_always });
        if (snapshot.tables.len != 1) return error.InvalidSqlBackendResponse;
        const table = snapshot.tables[0] orelse return error.TableNotFound;
        const definition = table.query_definition orelse return error.InvalidSqlBackendResponse;
        if (table.table_id == 0 or definition.table_id != table.table_id) return error.InvalidSqlBackendResponse;
        var binding = try self.server.sql_schema_cache.resolve(self.server.sqlPlanCacheIo(), alloc, definition.schema_json, table.table_id, table.name);
        binding.scope = .{ .database = try alloc.dupe(u8, target.database), .namespace = try alloc.dupe(u8, target.namespace), .name = try alloc.dupe(u8, target.table), .revision = snapshot.revision };
        self.revision = snapshot.revision;
        self.target = target;
        if (self.prepared_bindings) |bindings| try @import("sql_prepared.zig").Binding.verify(bindings, binding);
        if (self.collect_prepared_bindings) |bindings| {
            const current = try @import("sql_prepared.zig").Binding.from(binding);
            const existing = for (bindings.items) |entry| {
                if (std.mem.eql(u8, entry.database, current.database) and std.mem.eql(u8, entry.namespace, current.namespace) and std.mem.eql(u8, entry.name, current.name)) {
                    try @import("sql_prepared.zig").Binding.verify(&.{entry}, binding);
                    break true;
                }
            } else false;
            if (!existing) {
                if (bindings.items.len >= 64) return error.SqlProgramLimitExceeded;
                try bindings.append(alloc, current);
            }
        }
        if (self.identity.*) |*identity| try http_server.projectCatalogIdentity(self.server.alloc, identity, logical, table.name);
        return binding;
    }

    fn verify(self: *Adapter, alloc: std.mem.Allocator, table: catalog.Table) !void {
        try self.context.ensureActive();
        // A renamed/reused logical name never silently retargets a prepared
        // statement. The coordinator still applies its own durable auth fence.
        const scope = table.scope orelse return error.InvalidSqlBackendResponse;
        const target: system_catalog.Target = .{ .database = scope.database, .namespace = scope.namespace, .table = scope.name };
        const bytes = try self.server.source.systemCatalog(alloc, self.context, .{ .resolve_many = .{ .targets = &.{target}, .expected_revision = scope.revision } });
        const snapshot = try std.json.parseFromSliceLeaky(system_catalog.ResolvedMany, alloc, bytes, .{});
        if (snapshot.revision != scope.revision or snapshot.tables.len != 1) return error.CatalogGenerationChanged;
        const current = snapshot.tables[0] orelse return error.CatalogGenerationChanged;
        if (current.table_id != table.id or !std.mem.eql(u8, current.name, table.physical_name)) return error.CatalogGenerationChanged;
    }

    fn prepareScan(self: *Adapter, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !helpers.OwnedScanKeysRequest {
        try self.verify(alloc, table);
        if (request.index_equality) |probe| {
            if (self.range_reads == null or request.primary_key != null or request.after != null or request.primary_order or
                probe.name.len == 0 or probe.values.len == 0 or probe.values.len > 32) return error.UnsupportedSqlExecution;
        }
        if (self.identity.*) |identity| {
            if (!http_server.permissionsAllow(identity.permissions, .table, table.physical_name, .read)) return error.Forbidden;
        }
        const conditions = try alloc.alloc(db_types.RelationalRowQuery.Condition, request.conditions.len);
        for (request.conditions, conditions) |condition, *output| output.* = .{
            .column = condition.column,
            .op = switch (condition.op) {
                .neq => .ne,
                inline else => |tag| @field(@FieldType(db_types.RelationalRowQuery.Condition, "op"), @tagName(tag)),
            },
            .value = condition.value,
        };
        // Borrow the already validated typed plan through the native call.
        // Archive/network adapters alone encode it at their actual boundary.
        var scan_request = helpers.OwnedScanKeysRequest{
            .from = if (request.primary_key == null) request.after orelse "" else "",
            .opts = .{
                .include_range_proofs = self.range_reads != null,
                .sql_document_preimage = request.include_document,
                .include_content_hashes = request.include_primary_digest,
                .exclusive_to = true,
                .include_documents = true,
                .include_all_fields = false,
                .fields = request.fields,
                .limit = request.limit,
                .relational_query = .{
                    .fields = request.fields,
                    .conditions = conditions,
                    .index = if (request.index_equality) |probe| probe.name else null,
                    .lower = if (request.index_equality) |probe| .{ .values = probe.values } else null,
                    .upper = if (request.index_equality) |probe| .{ .values = probe.values } else null,
                    .auto_index = request.primary_key == null and request.index_equality == null and !request.primary_order,
                    .schema_version = table.schema_version,
                },
            },
        };
        scan_request.opts.execution_deadline_ns = (try self.context.platformDeadline()).deadline_ns;
        scan_request.opts.cancellation = self.context.cancellation;
        if (request.primary_key != null) {
            scan_request.opts.inclusive_from = true;
            scan_request.opts.exclusive_to = true;
        }
        if (try http_server.resolveEffectiveRowFilterJson(alloc, self.identity.*, table.physical_name)) |filter| {
            try http_server.injectRowFilterIntoScanRequest(alloc, &scan_request, filter);
        }
        // Routing spans are half-open. [key, key + NUL) includes exactly the
        // byte key even when a shard starts at key, without its descendants.
        if (request.primary_key) |key| {
            scan_request.from = key;
            scan_request.to = try pointUpperBound(alloc, key);
        }
        return scan_request;
    }

    const ReadCursor = struct {
        alloc: std.mem.Allocator,
        adapter: *Adapter,
        schema_version: u32,
        require_primary_digest: bool = false,
        view: @import("table_read_source.zig").RelationalReadView,

        fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
            const self: *ReadCursor = @ptrCast(@alignCast(ptr));
            try self.adapter.context.ensureActive();
            var page = try self.view.next(alloc, limit);
            errdefer page.deinit();
            if (page.rows.len > limit) return error.InvalidSqlBackendResponse;
            const rows = try page.arena.allocator().alloc(catalog.Row, page.rows.len);
            for (page.rows, rows) |row, *out| {
                if (row.schema_version != self.schema_version) return error.CatalogGenerationChanged;
                if (self.require_primary_digest and row.expected_content_digest == null) return error.InvalidSqlBackendResponse;
                out.* = .{ .id = row.id, .version = row.version, .value = row.value, .sql_nulls = row.sql_nulls, .expected_content_digest = row.expected_content_digest, .document = row.document };
            }
            return .{ .rows = rows, .after = page.after, .owned_arena = page.arena };
        }

        fn close(ptr: *anyopaque) void {
            const self: *ReadCursor = @ptrCast(@alignCast(ptr));
            self.view.deinit();
            self.alloc.destroy(self);
        }
    };

    fn openScan(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        if (self.range_reads != null) {
            const statement = try openStatement(ptr, alloc, &.{.{ .table = table, .request = request }});
            errdefer statement.close(statement.ptr);
            const wrapper = try alloc.create(SingleStatementCursor);
            wrapper.* = .{ .alloc = alloc, .statement = statement };
            return .{ .ptr = wrapper, .next = SingleStatementCursor.next, .close = SingleStatementCursor.close };
        }
        if (self.staged) |staged| {
            // A session SELECT needs one native statement snapshot. Multi-owner
            // sources without that guarantee remain explicitly unsupported.
            const row_filter = try http_server.resolveEffectiveRowFilterJson(alloc, self.identity.*, table.physical_name);
            defer if (row_filter) |filter| alloc.free(filter);
            var ordered = request;
            ordered.primary_order = true;
            const native_cursor = (try openNativeScan(ptr, alloc, table, ordered)) orelse return error.UnsupportedSqlExecution;
            errdefer native_cursor.close(native_cursor.ptr);
            return try @import("sql_session_overlay.zig").open(alloc, native_cursor, staged, table, ordered, row_filter);
        }
        return openNativeScan(ptr, alloc, table, request);
    }

    const SingleStatementCursor = struct {
        alloc: std.mem.Allocator,
        statement: catalog.StatementRead,
        fn next(raw: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const cursor = self.statement.cursors[0];
            return cursor.next(cursor.ptr, alloc, limit);
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.statement.close(self.statement.ptr);
            self.alloc.destroy(self);
        }
    };

    fn openNativeScan(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        if (request.index_equality != null) return error.SqlRangeTrackingRequired;
        const source = self.server.table_reads orelse return error.TableNotFound;
        if (source.vtable.open_relational_read == null or source.route_fence != null) return null;
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const scan_request = try self.prepareScan(scratch.allocator(), table, request);
        // The provider owns all request data it retains; only page values use
        // short-lived arenas. Closing the cursor releases its snapshot first.
        const view = (try source.openRelationalRead(alloc, table.physical_name, scan_request.from, scan_request.to, scan_request.opts, .read_index)) orelse return null;
        errdefer view.deinit();
        // Tie the acquired physical snapshot to the SQL binding, not merely
        // the provider's independently resolved routing fence. Replacement or
        // restore under the same physical name must fail before publication.
        try self.verify(scratch.allocator(), table);
        const cursor = try alloc.create(ReadCursor);
        cursor.* = .{ .alloc = alloc, .adapter = self, .schema_version = table.schema_version, .require_primary_digest = request.include_primary_digest, .view = view };
        return .{ .ptr = cursor, .next = ReadCursor.next, .close = ReadCursor.close };
    }

    const StatementRead = struct {
        alloc: std.mem.Allocator,
        native: @import("table_read_source.zig").RelationalStatementRead,
        wrappers: []ReadCursor,
        cursors: []catalog.Cursor,
        overlays: bool,

        fn borrowedClose(_: *anyopaque) void {}

        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.overlays) for (self.cursors) |cursor| cursor.close(cursor.ptr);
            self.native.deinit();
            self.alloc.free(self.cursors);
            self.alloc.free(self.wrappers);
            self.alloc.destroy(self);
        }
    };

    fn openStatement(ptr: *anyopaque, alloc: std.mem.Allocator, requests: []const catalog.StatementScan) !catalog.StatementRead {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        if (requests.len == 0 or requests.len > 64) return error.SqlProgramLimitExceeded;
        const read_source = self.server.table_reads orelse return error.TableNotFound;
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const scans = try temporary.alloc(@import("table_read_source.zig").RelationalStatementScan, requests.len);
        const overlay_staged = self.staged != null and self.staged.?.tables.len != 0;
        for (requests, scans) |request, *input| {
            var query_request = request.request;
            if (overlay_staged) query_request.primary_order = true;
            const scan_request = try self.prepareScan(temporary, request.table, query_request);
            input.* = .{ .table = request.table.physical_name, .from = scan_request.from, .to = scan_request.to, .opts = scan_request.opts };
        }
        const retained = try alloc.create(StatementRead);
        errdefer alloc.destroy(retained);
        const wrappers = try alloc.alloc(ReadCursor, requests.len);
        errdefer alloc.free(wrappers);
        const cursors = try alloc.alloc(catalog.Cursor, requests.len);
        errdefer alloc.free(cursors);
        const native = read_source.openRelationalStatement(alloc, scans, .read_index) catch |err| blk: {
            if (err != error.SqlRangeTrackingRequired or self.range_reads == null) return err;
            const writes = self.server.table_writes orelse return error.SqlRangeTrackingRequired;
            // Activation is a replicated, idempotent capability transition.
            // Try native capture first: an already active table needs no new
            // Raft proposal or additional catalog/control round trip.
            var activated: std.StringHashMapUnmanaged(void) = .empty;
            for (requests) |request| {
                if ((try activated.getOrPut(temporary, request.table.physical_name)).found_existing) continue;
                try self.verify(temporary, request.table);
                try writes.activateRangeTracking(temporary, request.table.physical_name, self.context);
            }
            break :blk try read_source.openRelationalStatement(alloc, scans, .read_index);
        };
        errdefer native.deinit();
        if (native.views.len != requests.len) return error.InvalidSqlBackendResponse;
        var wrapped: usize = 0;
        errdefer if (overlay_staged) for (cursors[0..wrapped]) |cursor| cursor.close(cursor.ptr);
        for (requests, native.views, wrappers, cursors) |request, view, *wrapper, *cursor| {
            try self.verify(temporary, request.table);
            if (self.range_reads) |observed| {
                const proofs = try native.rangeProofs(temporary, wrapped);
                if (proofs.len == 0) return error.InvalidSqlBackendResponse;
                if (request.request.index_equality != null) {
                    for (proofs) |owner| {
                        if (owner.proofs.len != 1 or owner.proofs[0].index == null) return error.InvalidSqlBackendResponse;
                    }
                }
                // Validate against every earlier statement before publishing
                // buffered output. Staged writes have not changed storage yet.
                if (self.staged) |staged| for (staged.tables) |old| {
                    if (!std.mem.eql(u8, staged.physicalName(old.table_name), request.table.physical_name)) continue;
                    if (old.schema_version != null and old.schema_version != request.table.schema_version) return error.CatalogGenerationChanged;
                    if (old.range_guards) |guards| {
                        var checked = try @import("range_read_guards.zig").merge(temporary, guards.value, proofs);
                        checked.deinit();
                    }
                };
                const scope = request.table.scope orelse return error.InvalidSqlBackendResponse;
                const logical = try (system_catalog.Target{ .database = scope.database, .namespace = scope.namespace, .table = scope.name }).resourceNameAlloc(temporary);
                try observed.observeRanges(self.server.alloc, logical, request.table.physical_name, request.table.schema_version, proofs);
            }
            wrapper.* = .{ .alloc = alloc, .adapter = self, .schema_version = request.table.schema_version, .require_primary_digest = request.request.include_primary_digest, .view = view };
            cursor.* = .{ .ptr = wrapper, .next = ReadCursor.next, .close = StatementRead.borrowedClose };
            if (if (overlay_staged) self.staged else null) |staged| {
                const row_filter = try http_server.resolveEffectiveRowFilterJson(temporary, self.identity.*, request.table.physical_name);
                var ordered = request.request;
                ordered.primary_order = true;
                cursor.* = try @import("sql_session_overlay.zig").open(alloc, cursor.*, staged, request.table, ordered, row_filter);
            }
            wrapped += 1;
        }
        retained.* = .{ .alloc = alloc, .native = native, .wrappers = wrappers, .cursors = cursors, .overlays = overlay_staged };
        return .{ .ptr = retained, .cursors = cursors, .close = StatementRead.close };
    }

    fn scan(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !catalog.Page {
        // The stateless wire fallback is relational-only. Document SQL needs
        // its retained native view to preserve projection/null and TTL semantics.
        if (table.storage_mode != .relational or request.include_primary_digest) return error.UnsupportedSqlExecution;
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        const scan_request = try self.prepareScan(alloc, table, request);
        const source = self.server.table_reads orelse return error.TableNotFound;
        var response = (try source.scan(alloc, table.physical_name, scan_request.from, scan_request.to, scan_request.opts, .read_index)) orelse return error.TableNotFound;
        defer response.deinit(alloc);
        var rows: std.ArrayList(catalog.Row) = .empty;
        var lines = std.mem.splitScalar(u8, response.ndjson, '\n');
        while (lines.next()) |line| {
            if (line.len == 0) continue;
            if (rows.items.len >= request.limit) return error.InvalidSqlBackendResponse;
            const row = try std.json.parseFromSliceLeaky(wire.RelationalRow, alloc, line, .{ .parse_numbers = false, .allocate = .alloc_always });
            if (request.primary_key) |key| {
                if (rows.items.len != 0 or !std.mem.eql(u8, row._id, key)) return error.InvalidSqlBackendResponse;
            }
            if (row.schema_version != table.schema_version) return error.CatalogGenerationChanged;
            const sql_nulls = try alloc.alloc(bool, row.row.map.count());
            for (row.row.map.values(), sql_nulls) |value, *is_null| is_null.* = value == .null;
            for (row.json_null_fields orelse &.{}) |field| {
                const column = table.column(field) catch return error.InvalidSqlBackendResponse;
                if (column.type != .json) return error.InvalidSqlBackendResponse;
                const index = row.row.map.getIndex(field) orelse return error.InvalidSqlBackendResponse;
                if (!sql_nulls[index]) return error.InvalidSqlBackendResponse;
                sql_nulls[index] = false;
            }
            try rows.append(alloc, .{ .id = row._id, .version = try std.fmt.parseInt(u64, row.version, 10), .value = .{ .object = row.row.map }, .sql_nulls = sql_nulls });
        }
        return .{ .rows = rows.items, .after = if (request.primary_key == null and rows.items.len == request.limit) rows.items[rows.items.len - 1].id else null };
    }

    fn prepareMutations(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) ![]const catalog.Mutation {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        try self.verify(alloc, table);
        const images = @import("../sql/mutation_images.zig");
        const writes = try images.writes(db_types.BatchWrite, alloc, input);
        if (writes.len == 0) return input;
        const source = self.server.table_reads orelse return error.UnsupportedSqlExecution;
        const upper = try pointUpperBound(alloc, writes[0].key);
        const view = (try source.openRelationalRead(alloc, table.physical_name, writes[0].key, upper, .{
            .limit = 1,
            .inclusive_from = true,
            .exclusive_to = true,
            .relational_query = .{ .fields = &.{}, .schema_version = table.schema_version },
            .execution_deadline_ns = (try self.context.platformDeadline()).deadline_ns,
            .cancellation = self.context.cancellation,
        }, .read_index)) orelse return error.UnsupportedSqlExecution;
        defer view.deinit();
        const normalized = try view.normalize(alloc, writes);
        const result = try images.merge(alloc, input, normalized);
        // RETURNING requires SELECT policy on the resulting row as well as
        // write authority. Do not publish postimages that an ordinary read hides.
        if (try http_server.resolveEffectiveRowFilterJson(alloc, self.identity.*, table.physical_name)) |json| {
            var filter = try @import("../search/pattern_filter.zig").PreparedPatternFilter.init(alloc, json);
            defer filter.deinit();
            for (result) |mutation| if (mutation.row) |row| {
                if (!try filter.matchesJson(alloc, mutation.key, row)) return error.Forbidden;
            };
        }
        try self.verify(alloc, table);
        return result;
    }

    fn mutate(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) !catalog.MutationOutcome {
        return mutateInternal(ptr, alloc, table, input, false);
    }

    fn mutatePrepared(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) !catalog.MutationOutcome {
        return mutateInternal(ptr, alloc, table, input, true);
    }

    fn mutateInternal(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation, already_prepared: bool) !catalog.MutationOutcome {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        try self.verify(alloc, table);
        const guards = try conflictGuards(alloc, input);
        if (self.active_transaction) |id| {
            const txn = @import("transactions.zig");
            var writes: std.ArrayList(db_types.BatchWrite) = .empty;
            var deletes: std.ArrayList([]const u8) = .empty;
            const predicates = try alloc.alloc(db_types.TransactionVersionPredicate, input.len);
            for (input, predicates) |mutation, *predicate| {
                predicate.* = .{ .key = mutation.key, .expected_version = mutation.expected_version, .expected_content_digest = mutation.expected_content_digest };
                if (mutation.predicate_only) continue;
                if (self.inserting and mutation.expected_version == 0) if (self.staged) |staged| for (staged.tables) |existing| {
                    if (!std.mem.eql(u8, staged.physicalName(existing.table_name), table.physical_name)) continue;
                    for (existing.batch.writes) |write| if (std.mem.eql(u8, write.key, mutation.key)) return error.UniqueConstraintViolation;
                    for (existing.batch.deletes) |deleted| if (std.mem.eql(u8, deleted, mutation.key)) {
                        for (existing.predicates.items) |observed| if (std.mem.eql(u8, observed.key, mutation.key)) {
                            predicate.expected_version = observed.expected_version;
                            break;
                        };
                        break;
                    };
                };
                if (mutation.row) |row| {
                    try writes.append(alloc, .{ .key = mutation.key, .value = try std.json.Stringify.valueAlloc(alloc, row, .{}), .json_null_fields = if (table.storage_mode == .document) &.{} else mutation.json_null_fields });
                } else try deletes.append(alloc, mutation.key);
            }
            // Document mutations always cross preparation in the executor;
            // their physical JSON already encodes SQL-NULL-as-absence. Do not
            // interpret the stripped native metadata a second time.
            const normalized = if (!already_prepared and writes.items.len != 0 and table.storage_mode == .relational) blk: {
                const source = self.server.table_reads orelse return error.UnsupportedSqlExecution;
                const first_key = writes.items[0].key;
                const upper = try pointUpperBound(alloc, first_key);
                const view = (try source.openRelationalRead(alloc, table.physical_name, first_key, upper, .{
                    .limit = 1,
                    .inclusive_from = true,
                    .exclusive_to = true,
                    .relational_query = .{ .fields = &.{}, .schema_version = table.schema_version },
                    .execution_deadline_ns = (try self.context.platformDeadline()).deadline_ns,
                    .cancellation = self.context.cancellation,
                }, .read_index)) orelse return error.UnsupportedSqlExecution;
                defer view.deinit();
                try self.verify(alloc, table);
                break :blk try view.normalize(alloc, writes.items);
            } else writes.items;
            const scope = table.scope orelse return error.InvalidSqlBackendResponse;
            const logical = try (system_catalog.Target{ .database = scope.database, .namespace = scope.namespace, .table = scope.name }).resourceNameAlloc(alloc);
            var tables = [_]txn.TableCommitRequest{.{ .table_name = @constCast(logical), .schema_version = if (table.storage_mode == .document) table.schema_version else null, .relational_schema_version = if (table.storage_mode == .relational) table.schema_version else null, .batch = .{ .writes = normalized, .deletes = deletes.items }, .predicates = .{ .items = predicates, .capacity = predicates.len } }};
            defer if (tables[0].conflict_guards) |*owned| owned.deinit();
            if (guards) |value| try tables[0].mergeConflictGuards(alloc, value);
            var bindings = [_]txn.CatalogBinding{.{ .logical = logical, .physical = table.physical_name }};
            const statement = txn.OwnedTransactionCommitRequest{ .tables = &tables, .catalog_bindings = .{ .items = &bindings, .capacity = 1 } };
            if (!(try self.server.transactionRequestAuthorized(self.identity.*, statement))) return error.Forbidden;
            _ = (try self.server.txn_sessions.stageValidated(self.server.alloc, id, &statement, .{ .ptr = self, .validate = validateStaged })) orelse return error.SqlTransactionNotActive;
            self.ranges_staged = true;
            self.outcome_transaction_id = std.fmt.bytesToHex(id, .lower);
            return .committed;
        }
        var writes: std.ArrayList(db_types.BatchWrite) = .empty;
        var deletes: std.ArrayList([]const u8) = .empty;
        const predicates = try alloc.alloc(db_types.TransactionVersionPredicate, input.len);
        for (input, predicates) |mutation, *predicate| {
            predicate.* = .{ .key = mutation.key, .expected_version = mutation.expected_version, .expected_content_digest = mutation.expected_content_digest };
            if (mutation.predicate_only) continue;
            if (mutation.row) |row| {
                try writes.append(alloc, .{ .key = mutation.key, .value = try std.json.Stringify.valueAlloc(alloc, row, .{}), .json_null_fields = if (table.storage_mode == .document) &.{} else mutation.json_null_fields });
            } else try deletes.append(alloc, mutation.key);
        }
        var response = @import("public_table_http.zig").handleNativeTableBatch(alloc, table.physical_name, .{ .schema_version = if (table.storage_mode == .document) table.schema_version else null, .relational_schema_version = if (table.storage_mode == .relational) table.schema_version else null, .writes = writes.items, .deletes = deletes.items, .predicates = predicates, .integrity_commands = if (guards) |value| value.commands else &.{}, .relational_integrity_generation_set = if (guards) |value| value.generation_set else null }, self.server.tableApi(self.context)) catch
            return error.SqlMutationOutcomeUnknown;
        defer response.deinit(alloc);
        self.outcome_transaction_id = mutationTransactionId(response.body);
        if (response.status == 200 or response.status == 201) return .committed;
        if (response.status == 202) return try committedMutationOutcome(response.body);
        // Preserve ambiguity: a transport/API failure after native admission
        // is never converted into a retryable, definitely-aborted SQL result.
        const failure = classifyMutationFailure(response.status, response.body);
        self.outcome_transaction_id = failure.transaction_id;
        return failure.err;
    }

    fn validateStaged(ptr: *anyopaque, alloc: std.mem.Allocator, previous: ?*const @import("transactions.zig").OwnedTransactionCommitRequest, candidate: *@import("transactions.zig").OwnedTransactionCommitRequest, statement: *const @import("transactions.zig").OwnedTransactionCommitRequest) !void {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        // Persist read dependencies and writes in the same durable session CAS.
        // A failed second staging operation must never leave unfenced writes.
        if (self.range_reads) |guards| try candidate.retainRangeGuards(alloc, guards);
        try @import("transactions.zig").SessionRegistry.normalizeSqlStage(ptr, alloc, previous, candidate, statement);
        try @import("relational_session_statement.zig").validate(self.server, alloc, previous, candidate, statement, self.context);
    }
};

pub fn sqlState(err: anyerror) []const u8 {
    return diagnostic(err).code;
}

pub const diagnostic = @import("../sql/errors.zig").describe;
pub const diagnosticMessage = @import("../sql/errors.zig").message;

const MutationFailure = struct { err: anyerror, transaction_id: ?[32]u8 = null };

fn mutationTransactionId(body: []const u8) ?[32]u8 {
    var storage: [2048]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    const Receipt = struct { transaction_id: ?[]const u8 = null };
    var parsed = std.json.parseFromSlice(Receipt, fixed.allocator(), body, .{ .ignore_unknown_fields = true, .allocate = .alloc_if_needed }) catch return null;
    defer parsed.deinit();
    return copyTransactionId(parsed.value.transaction_id);
}

fn copyTransactionId(value: ?[]const u8) ?[32]u8 {
    const id = value orelse return null;
    if (id.len != 32) return null;
    for (id) |byte| if (!std.ascii.isHex(byte)) return null;
    var owned: [32]u8 = undefined;
    @memcpy(&owned, id);
    return owned;
}

fn pointUpperBound(alloc: std.mem.Allocator, key: []const u8) ![]u8 {
    return std.mem.concat(alloc, u8, &.{ key, &.{0} });
}

fn committedMutationOutcome(body: []const u8) !catalog.MutationOutcome {
    var storage: [2048]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    const Receipt = struct { status: []const u8, failure: ?struct { code: []const u8 } = null };
    var parsed = std.json.parseFromSlice(Receipt, fixed.allocator(), body, .{ .ignore_unknown_fields = true, .allocate = .alloc_if_needed }) catch return error.SqlMutationOutcomeUnknown;
    defer parsed.deinit();
    if (std.mem.eql(u8, parsed.value.status, "committed_pending")) return .committed_pending;
    if (std.mem.eql(u8, parsed.value.status, "committed_repair_required")) {
        if (parsed.value.failure) |failure| {
            if (std.mem.eql(u8, failure.code, "graph_metric_materialization_rejected")) return .committed_graph_metric_materialization_rejected;
        }
        return .committed_repair_required;
    }
    return error.SqlMutationOutcomeUnknown;
}

/// Receipt handling must not depend on heap headroom after a native commit.
/// Typed native definite conflicts remain definite; unrecognized outcomes do
/// not invite unsafe mutation replay.
fn classifyMutationFailure(status: u16, body: []const u8) MutationFailure {
    var storage: [2048]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    const Receipt = struct { code: ?[]const u8 = null, @"error": ?[]const u8 = null, status: ?[]const u8 = null, transaction_id: ?[]const u8 = null };
    var parsed = std.json.parseFromSlice(Receipt, fixed.allocator(), body, .{ .ignore_unknown_fields = true, .allocate = .alloc_if_needed }) catch {
        // Legacy plain text has only a deliberately small exact whitelist.
        // A malformed/oversized JSON receipt must not lose an unknown-outcome
        // marker and become a definitely-aborted result from its status alone.
        if (std.mem.startsWith(u8, std.mem.trimStart(u8, body, " \t\r\n"), "{")) return .{ .err = error.SqlMutationOutcomeUnknown };
        return .{ .err = definiteMutationFailure(status, body) orelse error.SqlMutationOutcomeUnknown };
    };
    defer parsed.deinit();
    const id = copyTransactionId(parsed.value.transaction_id);
    if (parsed.value.code) |code| {
        if (std.mem.eql(u8, code, "transaction_outcome_unknown") or std.mem.eql(u8, code, "write_outcome_unknown"))
            return .{ .err = error.SqlMutationOutcomeUnknown, .transaction_id = id };
    }
    // A commit/unknown receipt contradicts any definite-abort error field.
    if (parsed.value.status) |state| if (std.mem.startsWith(u8, state, "committed") or std.mem.eql(u8, state, "unknown"))
        return .{ .err = error.SqlMutationOutcomeUnknown, .transaction_id = id };
    return .{ .err = definiteMutationFailure(status, parsed.value.@"error" orelse "") orelse error.SqlMutationOutcomeUnknown, .transaction_id = id };
}

fn definiteMutationFailure(status: u16, native: []const u8) ?anyerror {
    if (status == 409) {
        if (std.mem.eql(u8, native, "batch transaction conflicted") or std.mem.eql(u8, native, "PreparedReadSetChanged") or std.mem.eql(u8, native, "VersionConflict")) return error.SqlWriteConflict;
        if (std.mem.eql(u8, native, "UniqueConstraintViolation")) return error.DuplicateSqlRow;
        if (std.mem.eql(u8, native, "ForeignKeyParentMissing") or std.mem.eql(u8, native, "ForeignKeyReferenced") or std.mem.eql(u8, native, "ForeignKeyMatchFullViolation")) return error.ForeignKeyViolation;
        if (std.mem.eql(u8, native, "standby is read-only")) return error.HAReadOnlyStandby;
        if (std.mem.eql(u8, native, "promoted standby requires primary open")) return error.HAPromotedStandbyRequiresPrimaryOpen;
        if (std.mem.eql(u8, native, "fenced primary rejects writes")) return error.HAFencedPrimary;
    }
    if (status == 400 or status == 409) {
        if (std.mem.eql(u8, native, "RelationalCheckViolation")) return error.RelationalCheckViolation;
        if (std.mem.eql(u8, native, "RelationalExpressionOverflow")) return error.RelationalExpressionOverflow;
        if (std.mem.eql(u8, native, "RelationalExpressionDivisionByZero")) return error.RelationalExpressionDivisionByZero;
    }
    if (status == 400) return error.SqlTypeMismatch;
    if (status == 403) return error.Forbidden;
    if (status == 404) return error.TableNotFound;
    if (status == 429) return error.SqlWriteCapacityUnavailable;
    if (status == 413 and std.mem.eql(u8, native, "RelationalIndexKeyTooLarge")) return error.RelationalIndexKeyTooLarge;
    return null;
}

pub fn httpStatus(err: anyerror) u16 {
    return diagnostic(err).httpStatus();
}

pub fn characterPosition(statement: []const u8, byte_offset: usize) ?i64 {
    const count = std.unicode.utf8CountCodepoints(statement[0..@min(byte_offset, statement.len)]) catch return null;
    return @intCast(count + 1);
}

test "SQL diagnostics preserve SQLSTATE and Unicode character positions" {
    try std.testing.expectEqualStrings("42601", sqlState(error.InvalidSqlSyntax));
    try std.testing.expectEqual(@as(u16, 501), httpStatus(error.UnsupportedSqlShape));
    try std.testing.expectEqual(@as(u16, 409), httpStatus(error.SqlMutationOutcomeUnknown));
    try std.testing.expectEqual(@as(?i64, 4), characterPosition("éé x", 5));
}

test "SQL point bound is half-open and excludes all byte-key descendants" {
    const key = "a\xff";
    const upper = try pointUpperBound(std.testing.allocator, key);
    defer std.testing.allocator.free(upper);
    try std.testing.expectEqualStrings("a\xff\x00", upper);
    try std.testing.expect(std.mem.order(u8, key, upper) == .lt);
    for (0..256) |next| {
        const descendant = [_]u8{ 'a', 0xff, @intCast(next) };
        try std.testing.expect(std.mem.order(u8, &descendant, upper) != .lt);
    }
}

test "SQL require-index equality uses exact native bounds only inside a guarded statement" {
    const Fake = struct {
        fn resolve(_: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, _: system_catalog.Call) ![]u8 {
            return alloc.dupe(u8, "{\"revision\":3,\"tables\":[{\"table_id\":7,\"name\":\"physical\"}]}");
        }
    };
    var fake: u8 = 0;
    var server: http_server.ApiHttpServer = undefined;
    server.source = .{ .ptr = &fake, .vtable = &.{ .status = undefined, .system_catalog = Fake.resolve } };
    var identity: ?http_server.AuthenticatedIdentity = null;
    var guarded: @import("transactions.zig").OwnedTransactionCommitRequest = .{};
    var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .range_reads = &guarded };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const table: catalog.Table = .{ .id = 7, .physical_name = "physical", .schema_version = 9, .columns = &.{}, .scope = .{ .database = "d", .namespace = "n", .name = "logical", .revision = 3 } };
    const values = [_]std.json.Value{.{ .string = "ready" }};
    const request: catalog.Scan = .{ .fields = &.{}, .index_equality = .{ .name = "label_idx", .values = &values }, .limit = 17 };
    const scan = try adapter.prepareScan(alloc, table, request);
    try std.testing.expectEqualStrings("", scan.from);
    try std.testing.expectEqualStrings("", scan.to);
    try std.testing.expectEqualStrings("label_idx", scan.opts.relational_query.?.index.?);
    try std.testing.expectEqualDeep(&values, scan.opts.relational_query.?.lower.?.values);
    try std.testing.expectEqualDeep(&values, scan.opts.relational_query.?.upper.?.values);
    try std.testing.expect(!scan.opts.relational_query.?.auto_index);
    const composite_values = [_]std.json.Value{ .{ .string = "ready" }, .{ .integer = 7 } };
    const composite = try adapter.prepareScan(alloc, table, .{ .fields = &.{}, .index_equality = .{ .name = "label_tenant_idx", .values = &composite_values }, .limit = 17 });
    try std.testing.expectEqualDeep(&composite_values, composite.opts.relational_query.?.lower.?.values);
    try std.testing.expectEqualDeep(&composite_values, composite.opts.relational_query.?.upper.?.values);
    adapter.active_transaction = @splat(1);
    adapter.staged = &guarded;
    try std.testing.expect(adapter.backend().coordinated_point_reads);
    try std.testing.expect(adapter.backend().coordinated_index_reads);
    var staged_table: @import("transactions.zig").TableCommitRequest = .{ .table_name = @constCast("physical") };
    guarded.tables = (&staged_table)[0..1];
    try std.testing.expect(adapter.backend().coordinated_point_reads);
    try std.testing.expect(!adapter.backend().coordinated_index_reads);
    adapter.range_reads = null;
    try std.testing.expectError(error.UnsupportedSqlExecution, adapter.prepareScan(alloc, table, request));
}

test "SQL API document preparation uses native normalization and retains mutation fences" {
    const read_source = @import("table_read_source.zig");
    const View = read_source.RelationalReadView;
    const Fake = struct {
        normalized: bool = false,
        closed: bool = false,
        fn resolve(_: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, _: system_catalog.Call) ![]u8 {
            return alloc.dupe(u8, "{\"revision\":3,\"tables\":[{\"table_id\":7,\"name\":\"physical\"}]}");
        }
        fn open(ptr: *anyopaque, _: std.mem.Allocator, name: []const u8, from: []const u8, to: []const u8, options: db_types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?View {
            try std.testing.expectEqualStrings("physical", name);
            try std.testing.expectEqualStrings("a", from);
            try std.testing.expectEqualStrings("a\x00", to);
            try std.testing.expectEqual(@as(?u32, 9), options.relational_query.?.schema_version);
            return .{ .ptr = ptr, .vtable = &.{ .next = undefined, .normalize = normalize, .close = close } };
        }
        fn normalize(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const db_types.BatchWrite) ![]db_types.BatchWrite {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), writes.len);
            try std.testing.expectEqual(@as(usize, 1), writes[0].json_null_fields.len);
            try std.testing.expectEqualStrings("j", writes[0].json_null_fields[0]);
            self.normalized = true;
            return alloc.dupe(db_types.BatchWrite, &.{.{ .key = "a", .value = "{\"j\":null,\"undeclared\":9007199254740993}", .json_null_fields = &.{"j"} }});
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closed = true;
        }
    };
    var fake: Fake = .{};
    var server: http_server.ApiHttpServer = undefined;
    server.source = .{ .ptr = &fake, .vtable = &.{ .status = undefined, .system_catalog = Fake.resolve } };
    server.table_reads = .{ .ptr = &fake, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .open_relational_read = Fake.open } };
    var identity: ?http_server.AuthenticatedIdentity = null;
    var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{} };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const table: catalog.Table = .{ .id = 7, .physical_name = "physical", .schema_version = 9, .storage_mode = .document, .columns = &.{.{ .name = "j", .path = "j", .type = .json }}, .scope = .{ .database = "d", .namespace = "n", .name = "logical", .revision = 3 } };
    const input: catalog.Mutation = .{ .key = "a", .row = try std.json.parseFromSliceLeaky(std.json.Value, alloc, "{\"j\":null}", .{}), .json_null_fields = &.{"j"}, .expected_version = 42, .expected_content_digest = @splat(8) };
    const output = try Adapter.prepareMutations(&adapter, alloc, table, &.{input});
    try std.testing.expect(fake.normalized and fake.closed);
    try std.testing.expectEqual(input.expected_version, output[0].expected_version);
    try std.testing.expectEqual(input.expected_content_digest, output[0].expected_content_digest);
    try std.testing.expect(output[0].row.?.object.contains("undeclared"));
    try std.testing.expectEqualStrings("j", output[0].json_null_fields[0]);
}

test "SQL API guarded sessions retain reads and atomic MERGE writes across transaction boundaries" {
    const reads = @import("table_read_source.zig");
    const contract = @import("distributed_txn_contract.zig");
    const metadata = @import("../metadata/api.zig");
    const compiler = @import("../sql/compiler.zig");
    const View = reads.RelationalReadView;
    const Fake = struct {
        const schema = "{\"version\":7,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"n\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}";
        active: bool = false,
        generation: u64 = 1,
        activations: usize = 0,
        commits: usize = 0,
        pages: usize = 0,
        closes: usize = 0,
        normalizations: usize = 0,
        emit_row: bool = false,
        point_absent: bool = false,
        expect_delete: bool = false,
        expect_insert: bool = false,
        expect_update: bool = false,
        apply_default: bool = false,
        concurrent_insert: bool = false,
        proof_calls: usize = 0,
        fail_proof_call: ?usize = null,
        point_scans: usize = 0,
        full_scans: usize = 0,
        unknown_commit: bool = false,
        views: [2]View = undefined,
        records: [1]@import("../common/topology_records.zig").TableRecord = .{.{ .table_id = 3, .name = "physical", .schema_json = schema }},
        fn status(_: *anyopaque) !metadata.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn resolve(_: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: system_catalog.Call) ![]u8 {
            if (call == .write_validation) return std.json.Stringify.valueAlloc(alloc, .{ .schema_json = schema }, .{});
            return std.json.Stringify.valueAlloc(alloc, .{ .revision = 2, .tables = .{.{ .table_id = 3, .name = "physical", .query_definition = .{ .table_id = 3, .schema_json = schema, .read_schema_json = "", .indexes_json = "{}" } }}, .logical_names = .{"docs"} }, .{});
        }
        fn snapshot(ptr: *anyopaque) !metadata.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .status = try status(ptr), .tables = &self.records, .ranges = &.{}, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn freeSnapshot(_: *anyopaque, _: *metadata.AdminSnapshot) void {}
        fn activate(ptr: *anyopaque, _: std.mem.Allocator, table: []const u8, _: operation.RequestContext) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("physical", table);
            self.active = true;
            self.activations += 1;
        }
        fn open(ptr: *anyopaque, _: std.mem.Allocator, scans: []const reads.RelationalStatementScan, _: @import("../raft/read_gate.zig").ReadConsistency) !reads.RelationalStatementRead {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(scans.len == 1 or scans.len == 2);
            for (scans) |scan_request| {
                if (scan_request.from.len == 0) self.full_scans += 1 else self.point_scans += 1;
            }
            self.point_absent = scans.len == 1 and std.mem.eql(u8, scans[0].from, "b");
            for (scans) |scan_request| try std.testing.expect(scan_request.opts.include_range_proofs);
            if (!self.active) return error.SqlRangeTrackingRequired;
            for (self.views[0..scans.len]) |*view| view.* = .{ .ptr = ptr, .vtable = &.{ .next = next, .close = close } };
            return .{ .ptr = ptr, .views = self.views[0..scans.len], .vtable = &.{ .close = close, .range_proofs = proofs } };
        }
        fn proofs(ptr: *anyopaque, alloc: std.mem.Allocator, scan_index: usize) ![]reads.RelationalStatementRead.OwnerRangeProof {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = scan_index;
            self.proof_calls += 1;
            if (self.fail_proof_call == self.proof_calls) return error.TestProofUnavailable;
            const observations = try alloc.dupe(@import("range_read_guards.zig").Proof, &.{.{ .bucket = 98, .generation = self.generation }});
            return alloc.dupe(reads.RelationalStatementRead.OwnerRangeProof, &.{.{ .fence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 2, .table_id = 3, .topology_epoch = 4, .route = .{ .group_id = 5, .range_id = 6, .identity_namespace = .{ .table_id = 3, .shard_id = 5, .range_id = 6 } } }, .proofs = observations }});
        }
        fn next(ptr: *anyopaque, alloc: std.mem.Allocator, _: u32) !View.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.pages += 1;
            if (self.emit_row and !self.point_absent) {
                var arena = std.heap.ArenaAllocator.init(alloc);
                errdefer arena.deinit();
                const rows = try arena.allocator().alloc(View.Row, 1);
                rows[0] = .{ .id = "a", .version = 42, .schema_version = 7, .value = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"n\":1}", .{}), .expected_content_digest = @splat(9) };
                return .{ .arena = arena, .rows = rows, .after = null };
            }
            return .{ .arena = std.heap.ArenaAllocator.init(alloc), .rows = &.{}, .after = null };
        }
        fn openNormalize(ptr: *anyopaque, _: std.mem.Allocator, table: []const u8, from: []const u8, to: []const u8, opts: db_types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?View {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("physical", table);
            const key: []const u8 = if (self.expect_update) "a" else "b";
            try std.testing.expectEqualStrings(key, from);
            try std.testing.expectEqualStrings(if (self.expect_update) "a\x00" else "b\x00", to);
            try std.testing.expectEqual(@as(?u32, 7), opts.relational_query.?.schema_version);
            return .{ .ptr = ptr, .vtable = &.{ .next = undefined, .normalize = normalize, .close = close } };
        }
        fn normalize(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const db_types.BatchWrite) ![]db_types.BatchWrite {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), writes.len);
            try std.testing.expectEqualStrings(if (self.expect_update) "a" else "b", writes[0].key);
            self.normalizations += 1;
            if (self.apply_default) {
                try std.testing.expectEqualStrings("{}", writes[0].value);
                return alloc.dupe(db_types.BatchWrite, &.{.{ .key = writes[0].key, .value = "{\"n\":5}" }});
            }
            if (self.expect_update) return alloc.dupe(db_types.BatchWrite, &.{.{ .key = "a", .value = "{\"n\":3}" }});
            return alloc.dupe(db_types.BatchWrite, writes);
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closes += 1;
        }
        fn commit(ptr: *anyopaque, _: std.mem.Allocator, _: db_types.TxnId, _: u64, tables: []const contract.TableCommitRequest, _: db_types.SyncLevel) !?contract.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.unknown_commit) return error.TestTransportFailure;
            try std.testing.expectEqual(@as(usize, 1), tables.len);
            try std.testing.expectEqualStrings("physical", tables[0].table_name);
            try std.testing.expectEqual(@as(usize, if (self.expect_insert or self.expect_update) 1 else 0), tables[0].writes.len);
            try std.testing.expectEqual(@as(usize, if (self.expect_delete) 1 else 0), tables[0].deletes.len);
            if (self.expect_insert) {
                try std.testing.expectEqualStrings("b", tables[0].writes[0].key);
                if (self.apply_default) try std.testing.expectEqualStrings("{\"n\":5}", tables[0].writes[0].value);
                try std.testing.expectEqual(@as(usize, 1), tables[0].predicates.len);
                try std.testing.expectEqualStrings("b", tables[0].predicates[0].key);
                try std.testing.expectEqual(@as(u64, 0), tables[0].predicates[0].expected_version);
            }
            if (self.expect_update) {
                try std.testing.expectEqualStrings("a", tables[0].writes[0].key);
                try std.testing.expectEqualStrings(if (self.apply_default) "{\"n\":5}" else "{\"n\":3}", tables[0].writes[0].value);
                try std.testing.expectEqual(@as(usize, 1), tables[0].predicates.len);
                try std.testing.expectEqual(@as(u64, 42), tables[0].predicates[0].expected_version);
            }
            if (self.expect_delete) {
                try std.testing.expectEqualStrings("a", tables[0].deletes[0]);
                try std.testing.expectEqual(@as(usize, 1), tables[0].predicates.len);
                try std.testing.expectEqual(@as(u64, 42), tables[0].predicates[0].expected_version);
                try std.testing.expectEqual(@as(?[32]u8, @splat(9)), tables[0].predicates[0].expected_content_digest);
            }
            try std.testing.expectEqual(@as(usize, 1), tables[0].range_guards.len);
            try std.testing.expectEqual(@as(?u64, 1), tables[0].range_guards[0].proofs[0].generation);
            if (self.concurrent_insert) return .{ .conflict = .{ .table_name = "physical", .key = "b", .message = "concurrent insert", .retryable = true } };
            self.commits += 1;
            return .{ .committed = .{ .participant_count = 1 } };
        }
        fn run(adapter: *Adapter, text: []const u8) !void {
            var compiled = try compiler.compile(std.testing.allocator, text, .{});
            defer compiled.deinit();
            var result = try adapter.execute(std.testing.allocator, &compiled, &.{}, .{}, null);
            defer result.deinit();
        }
    };
    var fake: Fake = .{};
    var server = http_server.ApiHttpServer.init(std.testing.allocator, .{}, .{ .ptr = &fake, .vtable = &.{ .status = Fake.status, .system_catalog = Fake.resolve, .supports_query_definitions = true, .admin_snapshot = Fake.snapshot, .free_admin_snapshot = Fake.freeSnapshot } }, .{ .ptr = &fake, .supports_sql_range_guards = true, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .open_relational_statement = Fake.open, .open_relational_read = Fake.openNormalize } }, .{ .ptr = &fake, .supports_sql_range_guards = true, .vtable = &.{ .batch = undefined, .activate_range_tracking = Fake.activate, .commit_transaction_with_id = Fake.commit } });
    defer server.deinit();
    var identity: ?http_server.AuthenticatedIdentity = null;
    var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{} };
    // A provider flag is necessary; merely having callbacks is insufficient.
    server.table_writes.?.supports_sql_range_guards = false;
    try std.testing.expectError(error.UnsupportedSqlExecution, Fake.run(&adapter, "BEGIN ISOLATION LEVEL SERIALIZABLE"));
    try std.testing.expectError(error.SqlRangeTrackingRequired, Fake.run(&adapter, "MERGE INTO docs d USING docs s ON d._id=s._id WHEN MATCHED THEN DELETE"));
    try std.testing.expect(adapter.result_session_id == null);
    server.table_writes.?.supports_sql_range_guards = true;
    try Fake.run(&adapter, "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY");
    var session = adapter.result_session_id.?;
    adapter.session_id = &session;
    // Pgwire's mutable lookup path must not rewrite the durable transaction
    // owner scope, including native timing and savepoint commands.
    adapter.session_namespace = "public";
    adapter.namespace = "analytics";
    try Fake.run(&adapter, "SET CONSTRAINTS ALL DEFERRED");
    try Fake.run(&adapter, "SAVEPOINT before_read");
    try Fake.run(&adapter, "SET CONSTRAINTS ALL IMMEDIATE");
    try Fake.run(&adapter, "SELECT n FROM docs");
    try Fake.run(&adapter, "ROLLBACK TO before_read");
    {
        const id = try @import("distributed_txn.zig").parseTxnIdHex(&session);
        var staged = try server.txn_sessions.cloneSqlStaged(std.testing.allocator, id);
        defer staged.deinit(std.testing.allocator);
        try std.testing.expect(staged.constraint_timing.items[0].deferred);
    }
    // Read observations cannot be erased by rolling back a savepoint.
    try Fake.run(&adapter, "COMMIT");
    try std.testing.expectEqual(@as(usize, 1), fake.commits);
    try std.testing.expectEqual(@as(usize, 1), fake.activations);
    adapter.session_id = null;
    try Fake.run(&adapter, "BEGIN ISOLATION LEVEL READ COMMITTED");
    session = adapter.result_session_id.?;
    adapter.session_id = &session;
    try std.testing.expectError(error.SqlRangeTrackingRequired, Fake.run(&adapter, "MERGE INTO docs d USING docs s ON d._id=s._id WHEN MATCHED THEN DELETE"));
    try Fake.run(&adapter, "ROLLBACK");
    adapter.session_id = null;
    adapter.session_namespace = null;
    adapter.namespace = "public";
    try Fake.run(&adapter, "BEGIN ISOLATION LEVEL REPEATABLE READ");
    session = adapter.result_session_id.?;
    adapter.session_id = &session;
    try Fake.run(&adapter, "SELECT n FROM docs");
    const before = fake.pages;
    fake.generation += 1;
    try std.testing.expectError(error.SqlWriteConflict, Fake.run(&adapter, "SELECT n FROM docs"));
    try std.testing.expectEqual(before, fake.pages); // abort before fetching/exposing rows
    try Fake.run(&adapter, "ROLLBACK");
    try std.testing.expectEqual(@as(usize, 1), fake.commits);
    adapter.session_id = null;
    fake.generation = 1;
    fake.emit_row = true;
    fake.expect_delete = true;
    try Fake.run(&adapter, "BEGIN ISOLATION LEVEL SERIALIZABLE");
    session = adapter.result_session_id.?;
    adapter.session_id = &session;
    try Fake.run(&adapter, "DELETE FROM docs WHERE n = 1 RETURNING n");
    try Fake.run(&adapter, "COMMIT");
    try std.testing.expectEqual(@as(usize, 2), fake.commits);
    adapter.session_id = null;
    try Fake.run(&adapter, "BEGIN ISOLATION LEVEL SERIALIZABLE");
    session = adapter.result_session_id.?;
    adapter.session_id = &session;
    try Fake.run(&adapter, "MERGE INTO docs d USING docs s ON d._id=s._id WHEN MATCHED THEN DELETE");
    try Fake.run(&adapter, "COMMIT");
    try std.testing.expectEqual(@as(usize, 3), fake.commits);
    adapter.session_id = null;
    try Fake.run(&adapter, "MERGE INTO docs d USING docs s ON d._id=s._id WHEN MATCHED THEN DELETE");
    try std.testing.expectEqual(@as(usize, 4), fake.commits);
    try std.testing.expect(adapter.result_session_id == null);
    try std.testing.expectError(error.SqlDivisionByZero, Fake.run(&adapter, "MERGE INTO docs d USING docs s ON d._id=s._id WHEN MATCHED THEN UPDATE SET n=d.n/0"));
    try std.testing.expect(adapter.result_session_id == null);
    try std.testing.expectEqual(@as(usize, 4), fake.commits);
    fake.expect_delete = false;
    fake.expect_insert = true;
    const points_before_insert = fake.point_scans;
    const full_before_insert = fake.full_scans;
    try Fake.run(&adapter, "MERGE INTO docs d USING (SELECT 'b' AS _id, 2 AS n) s ON d._id=s._id WHEN NOT MATCHED THEN INSERT (_id,n) VALUES (s._id,s.n)");
    try std.testing.expectEqual(points_before_insert + 1, fake.point_scans);
    try std.testing.expectEqual(full_before_insert, fake.full_scans);
    try std.testing.expectEqual(@as(usize, 5), fake.commits);
    {
        var compiled = try compiler.compile(std.testing.allocator, "MERGE INTO docs d USING (SELECT 'b' AS _id, 2 AS n) s ON d._id=s._id WHEN NOT MATCHED THEN INSERT (_id,n) VALUES (s._id,s.n) RETURNING d._id, d.n, s.n AS source_n", .{});
        defer compiled.deinit();
        var result = try adapter.execute(std.testing.allocator, &compiled, &.{}, .{}, null);
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 3), result.output.columns.len);
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings("b", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("2", result.output.rows[0][1].string);
        try std.testing.expectEqualStrings("2", result.output.rows[0][2].string);
        try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
    }
    try std.testing.expectEqual(@as(usize, 6), fake.commits);
    try std.testing.expectError(error.SqlDivisionByZero, Fake.run(&adapter, "MERGE INTO docs d USING (SELECT 'b' AS _id, 2 AS n) s ON d._id=s._id WHEN NOT MATCHED THEN INSERT (_id,n) VALUES (s._id,s.n) RETURNING d.n/0 AS invalid_result"));
    try std.testing.expectEqual(@as(usize, 6), fake.commits);
    try std.testing.expect(adapter.result_session_id == null);
    fake.expect_insert = false;
    fake.expect_update = true;
    const before_returning_normalization = fake.normalizations;
    {
        var compiled = try compiler.compile(std.testing.allocator, "MERGE INTO docs d USING docs s ON d._id=s._id WHEN MATCHED THEN UPDATE SET n=d.n+1 RETURNING d.n AS updated_n, s.n AS source_n", .{});
        defer compiled.deinit();
        var result = try adapter.execute(std.testing.allocator, &compiled, &.{}, .{}, null);
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings("3", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("1", result.output.rows[0][1].string);
    }
    try std.testing.expectEqual(before_returning_normalization + 1, fake.normalizations);
    try std.testing.expectEqual(@as(usize, 7), fake.commits);
    fake.expect_update = false;
    fake.expect_delete = true;
    {
        var compiled = try compiler.compile(std.testing.allocator, "MERGE INTO docs d USING docs s ON d._id=s._id WHEN MATCHED THEN DELETE RETURNING d.n AS deleted_n, s.n AS source_n", .{});
        defer compiled.deinit();
        var result = try adapter.execute(std.testing.allocator, &compiled, &.{}, .{}, null);
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings("1", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("1", result.output.rows[0][1].string);
    }
    try std.testing.expectEqual(@as(usize, 8), fake.commits);
    fake.expect_delete = false;
    fake.expect_update = true;
    fake.apply_default = true;
    {
        var compiled = try compiler.compile(std.testing.allocator, "MERGE INTO docs d USING docs s ON d._id=s._id WHEN MATCHED THEN UPDATE SET n=DEFAULT RETURNING d.n AS defaulted_n, s.n AS source_n", .{});
        defer compiled.deinit();
        var result = try adapter.execute(std.testing.allocator, &compiled, &.{}, .{}, null);
        defer result.deinit();
        try std.testing.expectEqualStrings("5", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("1", result.output.rows[0][1].string);
    }
    try std.testing.expectEqual(@as(usize, 9), fake.commits);
    fake.expect_update = false;
    fake.expect_insert = true;
    {
        var compiled = try compiler.compile(std.testing.allocator, "MERGE INTO docs d USING (SELECT 'b' AS _id) s ON d._id=s._id WHEN NOT MATCHED THEN INSERT (_id,n) VALUES (s._id,DEFAULT) RETURNING d.n AS defaulted_n", .{});
        defer compiled.deinit();
        var result = try adapter.execute(std.testing.allocator, &compiled, &.{}, .{}, null);
        defer result.deinit();
        try std.testing.expectEqualStrings("5", result.output.rows[0][0].string);
    }
    try std.testing.expectEqual(@as(usize, 10), fake.commits);
    fake.apply_default = false;
    fake.concurrent_insert = true;
    try std.testing.expectError(error.SqlWriteConflict, Fake.run(&adapter, "MERGE INTO docs d USING (SELECT 'b' AS _id, 2 AS n) s ON d._id=s._id WHEN NOT MATCHED THEN INSERT (_id,n) VALUES (s._id,s.n)"));
    try std.testing.expectEqual(@as(usize, 10), fake.commits);
    fake.concurrent_insert = false;
    fake.expect_insert = false;
    fake.fail_proof_call = fake.proof_calls + 2;
    const closes_before_failed_capture = fake.closes;
    try std.testing.expectError(error.TestProofUnavailable, Fake.run(&adapter, "MERGE INTO docs d USING docs s ON d._id=s._id WHEN MATCHED THEN DELETE"));
    try std.testing.expect(fake.closes > closes_before_failed_capture);
    try std.testing.expectEqual(@as(usize, 10), fake.commits);
    try std.testing.expect(adapter.result_session_id == null);
    fake.fail_proof_call = null;
    fake.unknown_commit = true;
    try std.testing.expectError(error.SqlTransactionOutcomeUnknown, Fake.run(&adapter, "MERGE INTO docs d USING docs s ON d._id=s._id WHEN MATCHED THEN DELETE"));
    try std.testing.expect(adapter.outcome_transaction_id != null);
    try std.testing.expect(adapter.result_session_id != null);
}

test "SQL API cross-table MERGE retains both source and target range proofs" {
    const reads = @import("table_read_source.zig");
    const contract = @import("distributed_txn_contract.zig");
    const metadata = @import("../metadata/api.zig");
    const compiler = @import("../sql/compiler.zig");
    const View = reads.RelationalReadView;
    const Fake = struct {
        const Self = @This();
        const schema = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"status\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}}}";
        const State = struct {
            table_id: u64,
            expression_case: bool = false,
            conditional_case: bool = false,
            skip_case: bool = false,
            delete_case: bool = false,
            insert_returning_case: bool = false,
            expression_predicate_case: bool = false,
            grouped_predicate_case: bool = false,
            cte_case: bool = false,
            done: bool = false,
        };
        states: [2]State = undefined,
        views: [2]View = undefined,
        records: [3]@import("../common/topology_records.zig").TableRecord = .{
            .{ .table_id = 3, .name = "physical_usage", .schema_json = schema },
            .{ .table_id = 4, .name = "physical_source", .schema_json = schema },
            .{ .table_id = 5, .name = "physical_archive", .schema_json = schema },
        },
        captures: usize = 0,
        commits: usize = 0,
        commit_attempts: usize = 0,
        source_conflict: bool = false,
        unknown_commit: bool = false,
        fail_source_proof: bool = false,
        expression_case: bool = false,
        conditional_case: bool = false,
        skip_case: bool = false,
        delete_case: bool = false,
        returning_case: bool = false,
        insert_returning_case: bool = false,
        expression_predicate_case: bool = false,
        grouped_predicate_case: bool = false,
        cte_case: bool = false,
        archive_id: u64 = 5,
        target_guard: bool = false,
        source_guard: bool = false,
        fn status(_: *anyopaque) !metadata.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn resolve(ptr: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: system_catalog.Call) ![]u8 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (call == .write_validation) return std.json.Stringify.valueAlloc(alloc, .{ .schema_json = schema }, .{});
            if (call != .resolve_many) return error.TestUnexpectedCatalogCall;
            const request = call.resolve_many;
            if (request.expected_revision) |revision| try std.testing.expectEqual(@as(u64, 2), revision);
            if (request.storage_names.len != 0) {
                const logical = try alloc.alloc(?[]const u8, request.storage_names.len);
                for (request.storage_names, logical) |physical, *out| {
                    const name = if (std.mem.eql(u8, physical, "physical_usage")) "usage_records" else if (std.mem.eql(u8, physical, "physical_source")) "source_records" else if (std.mem.eql(u8, physical, "physical_archive")) "archived_records" else return error.TestUnexpectedTable;
                    out.* = try (system_catalog.Target{ .table = name }).resourceNameAlloc(alloc);
                }
                return std.json.Stringify.valueAlloc(alloc, system_catalog.ResolvedMany{ .revision = 2, .tables = &.{}, .logical_names = logical }, .{});
            }
            const tables = try alloc.alloc(?system_catalog.ResolvedTable, request.targets.len);
            for (request.targets, tables) |target, *out| {
                const is_usage = std.mem.eql(u8, target.table, "usage_records");
                try std.testing.expect(is_usage or std.mem.eql(u8, target.table, "source_records") or std.mem.eql(u8, target.table, "archived_records"));
                const is_archive = std.mem.eql(u8, target.table, "archived_records");
                const id: u64 = if (is_usage) 3 else if (is_archive) self.archive_id else 4;
                out.* = .{ .table_id = id, .name = if (is_usage) "physical_usage" else if (is_archive) "physical_archive" else "physical_source", .query_definition = if (request.include_query_definitions) .{ .table_id = id, .schema_json = schema, .read_schema_json = "", .indexes_json = "{}" } else null };
            }
            return std.json.Stringify.valueAlloc(alloc, system_catalog.ResolvedMany{ .revision = 2, .tables = tables }, .{});
        }
        fn snapshot(ptr: *anyopaque) !metadata.AdminSnapshot {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return .{ .status = try status(ptr), .tables = &self.records, .ranges = &.{}, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn freeSnapshot(_: *anyopaque, _: *metadata.AdminSnapshot) void {}
        fn open(ptr: *anyopaque, _: std.mem.Allocator, scans: []const reads.RelationalStatementScan, _: @import("../raft/read_gate.zig").ReadConsistency) !reads.RelationalStatementRead {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 2), scans.len);
            self.captures += 1;
            for (scans, &self.states, &self.views) |scan_request, *state, *view| {
                try std.testing.expect(scan_request.opts.include_range_proofs);
                const table_id: u64 = if (std.mem.eql(u8, scan_request.table, "physical_usage")) 3 else if (std.mem.eql(u8, scan_request.table, "physical_source")) 4 else if (std.mem.eql(u8, scan_request.table, "physical_archive")) 5 else return error.TestUnexpectedTable;
                state.* = .{ .table_id = table_id, .expression_case = self.expression_case, .conditional_case = self.conditional_case, .skip_case = self.skip_case, .delete_case = self.delete_case, .insert_returning_case = self.insert_returning_case, .expression_predicate_case = self.expression_predicate_case, .grouped_predicate_case = self.grouped_predicate_case, .cte_case = self.cte_case };
                view.* = .{ .ptr = state, .vtable = &.{ .next = next, .close = close } };
            }
            return .{ .ptr = self, .views = &self.views, .vtable = &.{ .close = close, .range_proofs = proofs } };
        }
        fn proofs(ptr: *anyopaque, alloc: std.mem.Allocator, index: usize) ![]reads.RelationalStatementRead.OwnerRangeProof {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const table_id = self.states[index].table_id;
            if (table_id != 3 and self.fail_source_proof) return error.TestProofUnavailable;
            const group_id: u64 = if (table_id == 3) 5 else if (table_id == 4) 7 else 9;
            const range_id: u64 = if (table_id == 3) 6 else if (table_id == 4) 8 else 10;
            const observations = try alloc.dupe(@import("range_read_guards.zig").Proof, &.{.{ .bucket = @intCast(table_id), .generation = 1 }});
            return alloc.dupe(reads.RelationalStatementRead.OwnerRangeProof, &.{.{ .fence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 2, .table_id = table_id, .topology_epoch = 4, .route = .{ .group_id = group_id, .range_id = range_id, .identity_namespace = .{ .table_id = table_id, .shard_id = group_id, .range_id = range_id } } }, .proofs = observations }});
        }
        fn next(ptr: *anyopaque, alloc: std.mem.Allocator, _: u32) !View.Page {
            const state: *State = @ptrCast(@alignCast(ptr));
            if (state.done) return .{ .arena = std.heap.ArenaAllocator.init(alloc), .rows = &.{}, .after = null };
            state.done = true;
            var arena = std.heap.ArenaAllocator.init(alloc);
            errdefer arena.deinit();
            const count: usize = if (state.table_id == 3) 1 else 2;
            const rows = try arena.allocator().alloc(View.Row, count);
            for (rows, 0..) |*row, index| {
                const id = if (index == 0) "a" else "b";
                const row_status = if (state.table_id == 3) (if (state.skip_case) "locked" else if (state.expression_predicate_case) "MiXeD" else if (state.conditional_case) "open" else "old") else if (state.cte_case) (if (index == 0) "ready" else "ignored") else if (state.delete_case) (if (index == 0) "deleted" else "new") else if (state.insert_returning_case) (if (index == 0) "ignored" else "ready") else if (state.expression_predicate_case) (if (index == 0) "mixed" else "ready") else if (state.grouped_predicate_case) (if (index == 0) "ready" else "new") else if (state.expression_case) (if (index == 0) "MiXeD" else "ready") else if (state.skip_case) (if (index == 0) "updated" else "ignored") else if (state.conditional_case) (if (index == 0) "updated" else "ready") else if (index == 0) "updated" else "new";
                const value = try std.fmt.allocPrint(arena.allocator(), "{{\"id\":\"{s}\",\"status\":\"{s}\"}}", .{ id, row_status });
                row.* = .{ .id = if (state.table_id == 3) "row-a" else id, .version = 42, .schema_version = 1, .value = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), value, .{}), .expected_content_digest = @splat(9) };
            }
            return .{ .arena = arena, .rows = rows, .after = null };
        }
        fn openNormalize(ptr: *anyopaque, _: std.mem.Allocator, table: []const u8, _: []const u8, _: []const u8, _: db_types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?View {
            try std.testing.expectEqualStrings("physical_usage", table);
            return .{ .ptr = ptr, .vtable = &.{ .next = undefined, .normalize = normalize, .close = close } };
        }
        fn normalize(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const db_types.BatchWrite) ![]db_types.BatchWrite {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, if (self.skip_case or self.delete_case) 0 else if (self.returning_case or self.insert_returning_case or self.cte_case) 1 else 2), writes.len);
            return alloc.dupe(db_types.BatchWrite, writes);
        }
        fn close(_: *anyopaque) void {}
        fn commit(ptr: *anyopaque, alloc: std.mem.Allocator, _: db_types.TxnId, _: u64, tables: []const contract.TableCommitRequest, _: db_types.SyncLevel) !?contract.CommitOutcome {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 2), tables.len);
            for (tables) |table| {
                if (std.mem.eql(u8, table.table_name, "physical_usage")) {
                    self.target_guard = table.range_guards.len != 0;
                    for (table.range_guards) |guard| {
                        try std.testing.expectEqual(@as(u64, 3), guard.fence.table_id);
                        try std.testing.expectEqual(@as(u64, 3), guard.fence.route.identity_namespace.table_id);
                        try std.testing.expectEqual(@as(u64, 5), guard.fence.route.group_id);
                        try std.testing.expectEqual(@as(u64, 6), guard.fence.route.range_id);
                        try std.testing.expectEqual(@as(?u64, 1), guard.proofs[0].generation);
                    }
                    try std.testing.expectEqual(@as(usize, if (self.skip_case or self.delete_case) 0 else if (self.returning_case or self.insert_returning_case or self.cte_case) 1 else 2), table.writes.len);
                    try std.testing.expectEqual(@as(usize, if (self.skip_case) 0 else if (self.returning_case or self.delete_case or self.insert_returning_case or self.cte_case) 1 else 2), table.predicates.len);
                    try std.testing.expectEqual(@as(usize, @intFromBool(self.delete_case)), table.deletes.len);
                    if (self.delete_case) try std.testing.expectEqualStrings("row-a", table.deletes[0]);
                    var matched = false;
                    var inserted = false;
                    for (table.predicates) |predicate| {
                        if (std.mem.eql(u8, predicate.key, "row-a")) {
                            try std.testing.expectEqual(@as(u64, 42), predicate.expected_version);
                            try std.testing.expectEqual(@as(?[32]u8, @splat(9)), predicate.expected_content_digest);
                            matched = true;
                        } else {
                            try std.testing.expectEqual(@as(u64, 0), predicate.expected_version);
                            inserted = true;
                        }
                    }
                    if (self.returning_case or self.delete_case or self.cte_case) {
                        try std.testing.expect(matched and !inserted);
                    } else if (self.insert_returning_case) {
                        try std.testing.expect(!matched and inserted);
                    } else if (!self.skip_case) try std.testing.expect(matched and inserted);
                    for (table.writes) |write| {
                        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, write.value, .{});
                        defer parsed.deinit();
                        const id = parsed.value.object.get("id").?.string;
                        const status_value = parsed.value.object.get("status").?.string;
                        if (std.mem.eql(u8, id, "a")) {
                            try std.testing.expectEqualStrings("row-a", write.key);
                            try std.testing.expectEqualStrings(if (self.expression_case or self.expression_predicate_case) "mixed" else if (self.grouped_predicate_case or self.cte_case) "ready" else "updated", status_value);
                        } else {
                            try std.testing.expectEqualStrings("b", id);
                            try std.testing.expectEqualStrings(if (self.expression_case) "READY" else if (self.conditional_case or self.insert_returning_case or self.expression_predicate_case) "ready" else "new", status_value);
                        }
                    }
                } else if (std.mem.eql(u8, table.table_name, "physical_source") or std.mem.eql(u8, table.table_name, "physical_archive")) {
                    const source_id: u64 = if (self.cte_case) 5 else 4;
                    const source_group: u64 = if (self.cte_case) 9 else 7;
                    const source_range: u64 = if (self.cte_case) 10 else 8;
                    try std.testing.expectEqualStrings(if (self.cte_case) "physical_archive" else "physical_source", table.table_name);
                    self.source_guard = table.range_guards.len != 0;
                    for (table.range_guards) |guard| {
                        try std.testing.expectEqual(source_id, guard.fence.table_id);
                        try std.testing.expectEqual(source_id, guard.fence.route.identity_namespace.table_id);
                        try std.testing.expectEqual(source_group, guard.fence.route.group_id);
                        try std.testing.expectEqual(source_range, guard.fence.route.range_id);
                        try std.testing.expectEqual(@as(?u64, 1), guard.proofs[0].generation);
                    }
                    try std.testing.expectEqual(@as(usize, 0), table.writes.len);
                } else return error.TestUnexpectedTable;
            }
            self.commit_attempts += 1;
            if (self.source_conflict) return .{ .conflict = .{ .table_name = if (self.cte_case) "physical_archive" else "physical_source", .key = "b", .message = "source range changed", .retryable = true } };
            if (self.unknown_commit) return error.TestTransportFailure;
            self.commits += 1;
            return .{ .committed = .{ .participant_count = 2 } };
        }
    };
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, @embedFile("../sql/fixtures/sql_parity_inventory.json"), .{});
    defer parsed.deinit();
    const sql = for (parsed.value.object.get("entries").?.array.items) |entry| {
        if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-0579")) break entry.object.get("sql").?.string;
    } else return error.TestMissingCorpusCase;
    const expression_sql = for (parsed.value.object.get("entries").?.array.items) |entry| {
        if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-0585")) break entry.object.get("sql").?.string;
    } else return error.TestMissingCorpusCase;
    const conditional_sql = for (parsed.value.object.get("entries").?.array.items) |entry| {
        if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-0581")) break entry.object.get("sql").?.string;
    } else return error.TestMissingCorpusCase;
    const returning_sql = for (parsed.value.object.get("entries").?.array.items) |entry| {
        if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-0584")) break entry.object.get("sql").?.string;
    } else return error.TestMissingCorpusCase;
    var fake: Fake = .{};
    var backend_runtime = try @import("../storage/background_runtime.zig").BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer backend_runtime.deinit();
    var prepared_backend = @import("../storage/mem_backend.zig").Backend.init(std.testing.allocator, .{});
    defer prepared_backend.close();
    var prepared_store = try prepared_backend.runtimeStore(std.testing.allocator, .{ .name = "merge-prepared-test" });
    defer prepared_store.deinit();
    var prepared_durable = @import("transactions.zig").DurableSessionStore.initRuntime(std.testing.allocator, &prepared_store);
    var server = http_server.ApiHttpServer.init(std.testing.allocator, .{ .backend_runtime = backend_runtime.ptr(), .session_store = &prepared_durable }, .{ .ptr = &fake, .vtable = &.{ .status = Fake.status, .system_catalog = Fake.resolve, .supports_query_definitions = true, .admin_snapshot = Fake.snapshot, .free_admin_snapshot = Fake.freeSnapshot } }, .{ .ptr = &fake, .supports_sql_range_guards = true, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .open_relational_statement = Fake.open, .open_relational_read = Fake.openNormalize } }, .{ .ptr = &fake, .supports_sql_range_guards = true, .vtable = &.{ .batch = undefined, .commit_transaction_with_id = Fake.commit } });
    defer server.deinit();
    var permissions = [_]@import("../usermgr/mod.zig").Permission{
        .{ .resource = @constCast("*"), .resource_type = .table, .type = .read },
        .{ .resource = @constCast("*"), .resource_type = .table, .type = .write },
    };
    var identity = try http_server.cloneCatalogIdentity(std.testing.allocator, .{ .username = @constCast("merge_writer"), .permissions = &permissions });
    defer if (identity) |*owned| owned.deinit(std.testing.allocator);
    var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{} };
    var compiled = try compiler.compile(std.testing.allocator, sql, .{});
    defer compiled.deinit();
    {
        var result = try adapter.execute(std.testing.allocator, &compiled, &.{}, .{}, null);
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
    }
    try std.testing.expectEqual(@as(usize, 1), fake.captures);
    try std.testing.expectEqual(@as(usize, 1), fake.commits);
    try std.testing.expectEqual(@as(usize, 1), fake.commit_attempts);
    try std.testing.expect(fake.target_guard and fake.source_guard);
    fake.source_conflict = true;
    try std.testing.expectError(error.SqlWriteConflict, adapter.execute(std.testing.allocator, &compiled, &.{}, .{}, null));
    try std.testing.expectEqual(@as(usize, 2), fake.captures);
    try std.testing.expectEqual(@as(usize, 2), fake.commit_attempts);
    try std.testing.expectEqual(@as(usize, 1), fake.commits);
    fake.source_conflict = false;
    fake.unknown_commit = true;
    try std.testing.expectError(error.SqlTransactionOutcomeUnknown, adapter.execute(std.testing.allocator, &compiled, &.{}, .{}, null));
    try std.testing.expectEqual(@as(usize, 3), fake.captures);
    try std.testing.expectEqual(@as(usize, 3), fake.commit_attempts);
    try std.testing.expectEqual(@as(usize, 1), fake.commits);
    fake.unknown_commit = false;
    fake.fail_source_proof = true;
    try std.testing.expectError(error.TestProofUnavailable, adapter.execute(std.testing.allocator, &compiled, &.{}, .{}, null));
    try std.testing.expectEqual(@as(usize, 4), fake.captures);
    try std.testing.expectEqual(@as(usize, 3), fake.commit_attempts);
    try std.testing.expectEqual(@as(usize, 1), fake.commits);
    fake.fail_source_proof = false;
    const target_resource = try (system_catalog.Target{ .table = "usage_records" }).resourceNameAlloc(std.testing.allocator);
    defer std.testing.allocator.free(target_resource);
    var target_only = [_]@import("../usermgr/mod.zig").Permission{
        .{ .resource = target_resource, .resource_type = .table, .type = .read },
        .{ .resource = target_resource, .resource_type = .table, .type = .write },
    };
    if (identity) |*owned| owned.deinit(std.testing.allocator);
    identity = null;
    identity = try http_server.cloneCatalogIdentity(std.testing.allocator, .{ .username = @constCast("merge_writer"), .permissions = &target_only });
    try std.testing.expectError(error.Forbidden, adapter.execute(std.testing.allocator, &compiled, &.{}, .{}, null));
    try std.testing.expectEqual(@as(usize, 4), fake.captures);
    try std.testing.expectEqual(@as(usize, 1), fake.commits);
    try std.testing.expectEqual(@as(usize, 3), fake.commit_attempts);
    const httpx = @import("httpx");
    var handler = @import("httpx_handler.zig").AntflyApiHandler{ .api_server = &server };
    for ([_]struct { id: []const u8, node: []const u8, source: []const u8 }{
        .{ .id = "sql-0068", .node = "Update", .source = "archived_records" },
        .{ .id = "sql-0069", .node = "Merge", .source = "source_records" },
    }) |case| {
        // The exact two-table UPDATE-with-subquery and MERGE plans bind both
        // catalog identities through HTTP without opening a statement capture
        // or entering the distributed commit path.
        const explain_sql = for (parsed.value.object.get("entries").?.array.items) |entry| {
            if (std.mem.eql(u8, entry.object.get("id").?.string, case.id)) break entry.object.get("sql").?.string;
        } else return error.TestMissingCorpusCase;
        const explain_body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = explain_sql }, .{});
        defer std.testing.allocator.free(explain_body);
        var explain_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql");
        defer explain_request.deinit();
        explain_request.body = explain_body;
        var explain_context = httpx.Context.init(std.testing.allocator, std.testing.io, &explain_request);
        defer explain_context.deinit();
        var explain_response = try handler.executeSQL(&explain_context);
        defer explain_response.deinit();
        if (explain_response.status.code != 200) std.debug.print("{s}: {s}\n", .{ case.id, explain_response.body orelse "" });
        try std.testing.expectEqual(@as(u16, 200), explain_response.status.code);
        const explained = try std.json.parseFromSlice(wire.SQLResponse, std.testing.allocator, explain_response.body.?, .{});
        defer explained.deinit();
        try std.testing.expectEqualStrings("EXPLAIN", explained.value.command_tag);
        try std.testing.expect(std.mem.indexOf(u8, explained.value.rows[0][0].string, case.node) != null);
        try std.testing.expect(std.mem.indexOf(u8, explained.value.rows[0][0].string, case.source) != null);
        try std.testing.expectEqual(@as(usize, 4), fake.captures);
        try std.testing.expectEqual(@as(usize, 3), fake.commit_attempts);
    }
    const body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = sql }, .{});
    defer std.testing.allocator.free(body);
    var request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql");
    defer request.deinit();
    request.body = body;
    var context = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
    defer context.deinit();
    var response = try handler.executeSQL(&context);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    const output = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, response.body.?, .{});
    defer output.deinit();
    try std.testing.expectEqual(@as(i64, 2), output.value.object.get("rows_affected").?.integer);
    try std.testing.expectEqual(@as(usize, 5), fake.captures);
    try std.testing.expectEqual(@as(usize, 4), fake.commit_attempts);
    try std.testing.expectEqual(@as(usize, 2), fake.commits);
    for ([_]struct { conflict: bool, code: []const u8, retryable: bool }{
        .{ .conflict = true, .code = "40001", .retryable = true },
        .{ .conflict = false, .code = "40003", .retryable = false },
    }) |fault| {
        fake.source_conflict = fault.conflict;
        fake.unknown_commit = !fault.conflict;
        var fault_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql");
        defer fault_request.deinit();
        fault_request.body = body;
        var fault_context = httpx.Context.init(std.testing.allocator, std.testing.io, &fault_request);
        defer fault_context.deinit();
        var fault_response = try handler.executeSQL(&fault_context);
        defer fault_response.deinit();
        try std.testing.expectEqual(@as(u16, 409), fault_response.status.code);
        const parsed_fault = try std.json.parseFromSlice(wire.SQLDiagnostic, std.testing.allocator, fault_response.body.?, .{});
        defer parsed_fault.deinit();
        try std.testing.expectEqualStrings(fault.code, parsed_fault.value.code);
        try std.testing.expectEqual(@as(?bool, fault.retryable), parsed_fault.value.retryable);
        if (!fault.conflict) try std.testing.expectEqual(@as(usize, 32), parsed_fault.value.transaction_id.?.len);
    }
    try std.testing.expectEqual(@as(usize, 7), fake.captures);
    try std.testing.expectEqual(@as(usize, 6), fake.commit_attempts);
    try std.testing.expectEqual(@as(usize, 2), fake.commits);
    fake.unknown_commit = false;
    var prepare_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql/prepared");
    defer prepare_request.deinit();
    prepare_request.body = body;
    var prepare_context = httpx.Context.init(std.testing.allocator, std.testing.io, &prepare_request);
    defer prepare_context.deinit();
    var prepare_response = try handler.prepareSQL(&prepare_context);
    defer prepare_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), prepare_response.status.code);
    const prepared = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, prepare_response.body.?, .{});
    defer prepared.deinit();
    const prepared_id = prepared.value.object.get("prepared_id").?.string;
    var durable_prepared = try @import("sql_prepared.zig").load(&prepared_durable, std.testing.allocator, prepared_id, "", server.localSessionNodeId(), 0);
    defer durable_prepared.deinit();
    try std.testing.expectEqual(@as(usize, 2), durable_prepared.value.bindings.len);
    try std.testing.expectEqualStrings("usage_records", durable_prepared.value.bindings[0].name);
    try std.testing.expectEqualStrings("source_records", durable_prepared.value.bindings[1].name);
    try std.testing.expectEqual(@as(u64, 3), durable_prepared.value.bindings[0].id);
    try std.testing.expectEqual(@as(u64, 4), durable_prepared.value.bindings[1].id);
    try std.testing.expectEqual(@as(usize, 7), fake.captures);
    try std.testing.expectEqual(@as(usize, 6), fake.commit_attempts);
    const execute_url = try std.fmt.allocPrint(std.testing.allocator, "http://127.0.0.1/db/v1/sql/prepared/{s}/execute", .{prepared_id});
    defer std.testing.allocator.free(execute_url);
    var execute_request = try httpx.Request.init(std.testing.allocator, .POST, execute_url);
    defer execute_request.deinit();
    execute_request.body = "{}";
    var execute_context = httpx.Context.init(std.testing.allocator, std.testing.io, &execute_request);
    defer execute_context.deinit();
    var execute_response = try handler.executePreparedSQL(&execute_context, prepared_id);
    defer execute_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), execute_response.status.code);
    const executed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, execute_response.body.?, .{});
    defer executed.deinit();
    try std.testing.expectEqual(@as(i64, 2), executed.value.object.get("rows_affected").?.integer);
    try std.testing.expectEqual(@as(usize, 8), fake.captures);
    try std.testing.expectEqual(@as(usize, 7), fake.commit_attempts);
    try std.testing.expectEqual(@as(usize, 3), fake.commits);
    fake.expression_case = true;
    const expression_body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = expression_sql }, .{});
    defer std.testing.allocator.free(expression_body);
    var expression_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql");
    defer expression_request.deinit();
    expression_request.body = expression_body;
    var expression_context = httpx.Context.init(std.testing.allocator, std.testing.io, &expression_request);
    defer expression_context.deinit();
    var expression_response = try handler.executeSQL(&expression_context);
    defer expression_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), expression_response.status.code);
    const expression_output = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, expression_response.body.?, .{});
    defer expression_output.deinit();
    try std.testing.expectEqual(@as(i64, 2), expression_output.value.object.get("rows_affected").?.integer);
    try std.testing.expectEqual(@as(usize, 9), fake.captures);
    try std.testing.expectEqual(@as(usize, 8), fake.commit_attempts);
    try std.testing.expectEqual(@as(usize, 4), fake.commits);
    fake.expression_case = false;
    fake.conditional_case = true;
    const conditional_body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = conditional_sql }, .{});
    defer std.testing.allocator.free(conditional_body);
    var conditional_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql");
    defer conditional_request.deinit();
    conditional_request.body = conditional_body;
    var conditional_context = httpx.Context.init(std.testing.allocator, std.testing.io, &conditional_request);
    defer conditional_context.deinit();
    var conditional_response = try handler.executeSQL(&conditional_context);
    defer conditional_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), conditional_response.status.code);
    const conditional_output = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, conditional_response.body.?, .{});
    defer conditional_output.deinit();
    try std.testing.expectEqual(@as(i64, 2), conditional_output.value.object.get("rows_affected").?.integer);
    try std.testing.expectEqual(@as(usize, 10), fake.captures);
    try std.testing.expectEqual(@as(usize, 9), fake.commit_attempts);
    try std.testing.expectEqual(@as(usize, 5), fake.commits);
    fake.skip_case = true;
    var skipped_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql");
    defer skipped_request.deinit();
    skipped_request.body = conditional_body;
    var skipped_context = httpx.Context.init(std.testing.allocator, std.testing.io, &skipped_request);
    defer skipped_context.deinit();
    var skipped_response = try handler.executeSQL(&skipped_context);
    defer skipped_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), skipped_response.status.code);
    const skipped_output = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, skipped_response.body.?, .{});
    defer skipped_output.deinit();
    try std.testing.expectEqual(@as(i64, 0), skipped_output.value.object.get("rows_affected").?.integer);
    try std.testing.expectEqual(@as(usize, 11), fake.captures);
    try std.testing.expectEqual(@as(usize, 10), fake.commit_attempts);
    try std.testing.expectEqual(@as(usize, 6), fake.commits);
    fake.conditional_case = false;
    fake.skip_case = false;
    fake.returning_case = true;
    const returning_body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = returning_sql }, .{});
    defer std.testing.allocator.free(returning_body);
    var returning_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql");
    defer returning_request.deinit();
    returning_request.body = returning_body;
    var returning_context = httpx.Context.init(std.testing.allocator, std.testing.io, &returning_request);
    defer returning_context.deinit();
    var returning_response = try handler.executeSQL(&returning_context);
    defer returning_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), returning_response.status.code);
    const returning_output = try std.json.parseFromSlice(wire.SQLResponse, std.testing.allocator, returning_response.body.?, .{});
    defer returning_output.deinit();
    try std.testing.expectEqual(@as(i64, 1), returning_output.value.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), returning_output.value.rows.len);
    try std.testing.expectEqualStrings("a", returning_output.value.rows[0][0].string);
    try std.testing.expectEqualStrings("updated", returning_output.value.rows[0][1].string);
    try std.testing.expectEqual(@as(usize, 12), fake.captures);
    try std.testing.expectEqual(@as(usize, 11), fake.commit_attempts);
    try std.testing.expectEqual(@as(usize, 7), fake.commits);
    fake.returning_case = false;
    fake.insert_returning_case = true;
    const insert_returning_sql = for (parsed.value.object.get("entries").?.array.items) |entry| {
        if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-0590")) break entry.object.get("sql").?.string;
    } else return error.TestMissingCorpusCase;
    const insert_returning_body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = insert_returning_sql }, .{});
    defer std.testing.allocator.free(insert_returning_body);
    const captures_before_insert_returning = fake.captures;
    const attempts_before_insert_returning = fake.commit_attempts;
    const commits_before_insert_returning = fake.commits;
    fake.target_guard = false;
    fake.source_guard = false;
    var insert_returning_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql");
    defer insert_returning_request.deinit();
    insert_returning_request.body = insert_returning_body;
    var insert_returning_context = httpx.Context.init(std.testing.allocator, std.testing.io, &insert_returning_request);
    defer insert_returning_context.deinit();
    var insert_returning_response = try handler.executeSQL(&insert_returning_context);
    defer insert_returning_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), insert_returning_response.status.code);
    const insert_returning_output = try std.json.parseFromSlice(wire.SQLResponse, std.testing.allocator, insert_returning_response.body.?, .{});
    defer insert_returning_output.deinit();
    try std.testing.expectEqual(@as(i64, 1), insert_returning_output.value.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), insert_returning_output.value.rows.len);
    try std.testing.expectEqualStrings("b", insert_returning_output.value.rows[0][0].string);
    try std.testing.expectEqualStrings("ready", insert_returning_output.value.rows[0][1].string);
    try std.testing.expectEqual(captures_before_insert_returning + 1, fake.captures);
    try std.testing.expectEqual(attempts_before_insert_returning + 1, fake.commit_attempts);
    try std.testing.expectEqual(commits_before_insert_returning + 1, fake.commits);
    try std.testing.expect(fake.target_guard and fake.source_guard);
    fake.insert_returning_case = false;
    var predicate_sql: [2][]const u8 = undefined;
    for ([_][]const u8{ "sql-0587", "sql-0588" }, 0..) |case_id, case_index| {
        predicate_sql[case_index] = for (parsed.value.object.get("entries").?.array.items) |entry| {
            if (std.mem.eql(u8, entry.object.get("id").?.string, case_id)) break entry.object.get("sql").?.string;
        } else return error.TestMissingCorpusCase;
        fake.expression_predicate_case = case_index == 0;
        fake.grouped_predicate_case = case_index == 1;
        const predicate_body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = predicate_sql[case_index] }, .{});
        defer std.testing.allocator.free(predicate_body);
        const captures_before_predicate = fake.captures;
        const attempts_before_predicate = fake.commit_attempts;
        const commits_before_predicate = fake.commits;
        fake.target_guard = false;
        fake.source_guard = false;
        var predicate_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql");
        defer predicate_request.deinit();
        predicate_request.body = predicate_body;
        var predicate_context = httpx.Context.init(std.testing.allocator, std.testing.io, &predicate_request);
        defer predicate_context.deinit();
        var predicate_response = try handler.executeSQL(&predicate_context);
        defer predicate_response.deinit();
        try std.testing.expectEqual(@as(u16, 200), predicate_response.status.code);
        const predicate_output = try std.json.parseFromSlice(wire.SQLResponse, std.testing.allocator, predicate_response.body.?, .{});
        defer predicate_output.deinit();
        try std.testing.expectEqual(@as(i64, 2), predicate_output.value.rows_affected);
        try std.testing.expectEqual(captures_before_predicate + 1, fake.captures);
        try std.testing.expectEqual(attempts_before_predicate + 1, fake.commit_attempts);
        try std.testing.expectEqual(commits_before_predicate + 1, fake.commits);
        try std.testing.expect(fake.target_guard and fake.source_guard);
    }
    fake.expression_predicate_case = false;
    fake.grouped_predicate_case = false;
    fake.cte_case = true;
    const cte_sql = for (parsed.value.object.get("entries").?.array.items) |entry| {
        if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-0623")) break entry.object.get("sql").?.string;
    } else return error.TestMissingCorpusCase;
    const cte_body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = cte_sql }, .{});
    defer std.testing.allocator.free(cte_body);
    const captures_before_cte = fake.captures;
    const attempts_before_cte = fake.commit_attempts;
    const commits_before_cte = fake.commits;
    fake.target_guard = false;
    fake.source_guard = false;
    var cte_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql");
    defer cte_request.deinit();
    cte_request.body = cte_body;
    var cte_context = httpx.Context.init(std.testing.allocator, std.testing.io, &cte_request);
    defer cte_context.deinit();
    var cte_response = try handler.executeSQL(&cte_context);
    defer cte_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), cte_response.status.code);
    const cte_output = try std.json.parseFromSlice(wire.SQLResponse, std.testing.allocator, cte_response.body.?, .{});
    defer cte_output.deinit();
    try std.testing.expectEqual(@as(i64, 1), cte_output.value.rows_affected);
    try std.testing.expectEqual(captures_before_cte + 1, fake.captures);
    try std.testing.expectEqual(attempts_before_cte + 1, fake.commit_attempts);
    try std.testing.expectEqual(commits_before_cte + 1, fake.commits);
    try std.testing.expect(fake.target_guard and fake.source_guard);
    var cte_prepare_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql/prepared");
    defer cte_prepare_request.deinit();
    cte_prepare_request.body = cte_body;
    var cte_prepare_context = httpx.Context.init(std.testing.allocator, std.testing.io, &cte_prepare_request);
    defer cte_prepare_context.deinit();
    var cte_prepare_response = try handler.prepareSQL(&cte_prepare_context);
    defer cte_prepare_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), cte_prepare_response.status.code);
    const cte_prepared = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, cte_prepare_response.body.?, .{});
    defer cte_prepared.deinit();
    const cte_prepared_id = cte_prepared.value.object.get("prepared_id").?.string;
    var cte_manifest = try @import("sql_prepared.zig").load(&prepared_durable, std.testing.allocator, cte_prepared_id, "", server.localSessionNodeId(), 0);
    defer cte_manifest.deinit();
    try std.testing.expectEqual(@as(usize, 2), cte_manifest.value.bindings.len);
    try std.testing.expectEqualStrings("usage_records", cte_manifest.value.bindings[0].name);
    try std.testing.expectEqual(@as(u64, 3), cte_manifest.value.bindings[0].id);
    try std.testing.expectEqualStrings("archived_records", cte_manifest.value.bindings[1].name);
    try std.testing.expectEqual(@as(u64, 5), cte_manifest.value.bindings[1].id);
    const cte_execute_url = try std.fmt.allocPrint(std.testing.allocator, "http://127.0.0.1/db/v1/sql/prepared/{s}/execute", .{cte_prepared_id});
    defer std.testing.allocator.free(cte_execute_url);
    var cte_execute_request = try httpx.Request.init(std.testing.allocator, .POST, cte_execute_url);
    defer cte_execute_request.deinit();
    cte_execute_request.body = "{}";
    var cte_execute_context = httpx.Context.init(std.testing.allocator, std.testing.io, &cte_execute_request);
    defer cte_execute_context.deinit();
    var cte_execute_response = try handler.executePreparedSQL(&cte_execute_context, cte_prepared_id);
    defer cte_execute_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), cte_execute_response.status.code);
    const cte_executed = try std.json.parseFromSlice(wire.SQLResponse, std.testing.allocator, cte_execute_response.body.?, .{});
    defer cte_executed.deinit();
    try std.testing.expectEqual(@as(i64, 1), cte_executed.value.rows_affected);
    try std.testing.expectEqual(captures_before_cte + 2, fake.captures);
    try std.testing.expectEqual(attempts_before_cte + 2, fake.commit_attempts);
    fake.archive_id = 6;
    fake.records[2].table_id = 6;
    var cte_stale_request = try httpx.Request.init(std.testing.allocator, .POST, cte_execute_url);
    defer cte_stale_request.deinit();
    cte_stale_request.body = "{}";
    var cte_stale_context = httpx.Context.init(std.testing.allocator, std.testing.io, &cte_stale_request);
    defer cte_stale_context.deinit();
    var cte_stale_response = try handler.executePreparedSQL(&cte_stale_context, cte_prepared_id);
    defer cte_stale_response.deinit();
    try std.testing.expectEqual(@as(u16, 409), cte_stale_response.status.code);
    try std.testing.expectEqual(captures_before_cte + 2, fake.captures);
    try std.testing.expectEqual(attempts_before_cte + 2, fake.commit_attempts);
    fake.archive_id = 5;
    fake.records[2].table_id = 5;
    var cte_compiled = try compiler.compile(std.testing.allocator, cte_sql, .{});
    defer cte_compiled.deinit();
    try std.testing.expectError(error.Forbidden, adapter.execute(std.testing.allocator, &cte_compiled, &.{}, .{}, null));
    try std.testing.expectEqual(captures_before_cte + 2, fake.captures);
    try std.testing.expectEqual(attempts_before_cte + 2, fake.commit_attempts);
    fake.cte_case = false;
    fake.skip_case = true;
    // Exact sql-0583 and sql-0589 keep a serializable read decision even
    // when ordered DO NOTHING arms produce no writes.
    for ([_][]const u8{ "sql-0583", "sql-0589" }) |case_id| {
        const exact_sql = for (parsed.value.object.get("entries").?.array.items) |entry| {
            if (std.mem.eql(u8, entry.object.get("id").?.string, case_id)) break entry.object.get("sql").?.string;
        } else return error.TestMissingCorpusCase;
        const no_op_body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = exact_sql }, .{});
        defer std.testing.allocator.free(no_op_body);
        const captures_before = fake.captures;
        const attempts_before = fake.commit_attempts;
        const commits_before = fake.commits;
        fake.target_guard = false;
        fake.source_guard = false;
        var no_op_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql");
        defer no_op_request.deinit();
        no_op_request.body = no_op_body;
        var no_op_context = httpx.Context.init(std.testing.allocator, std.testing.io, &no_op_request);
        defer no_op_context.deinit();
        var no_op_response = try handler.executeSQL(&no_op_context);
        defer no_op_response.deinit();
        try std.testing.expectEqual(@as(u16, 200), no_op_response.status.code);
        const no_op_output = try std.json.parseFromSlice(wire.SQLResponse, std.testing.allocator, no_op_response.body.?, .{});
        defer no_op_output.deinit();
        try std.testing.expectEqual(@as(i64, 0), no_op_output.value.rows_affected);
        try std.testing.expectEqual(captures_before + 1, fake.captures);
        try std.testing.expectEqual(attempts_before + 1, fake.commit_attempts);
        try std.testing.expectEqual(commits_before + 1, fake.commits);
        try std.testing.expect(fake.target_guard and fake.source_guard);
    }
    fake.skip_case = false;
    fake.delete_case = true;
    // Exact sql-0582 deletes only the matched target row. The source range
    // remains a read participant in the same guarded transaction.
    const delete_sql = for (parsed.value.object.get("entries").?.array.items) |entry| {
        if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-0582")) break entry.object.get("sql").?.string;
    } else return error.TestMissingCorpusCase;
    const delete_body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = delete_sql }, .{});
    defer std.testing.allocator.free(delete_body);
    const captures_before_delete = fake.captures;
    const attempts_before_delete = fake.commit_attempts;
    const commits_before_delete = fake.commits;
    fake.target_guard = false;
    fake.source_guard = false;
    var delete_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql");
    defer delete_request.deinit();
    delete_request.body = delete_body;
    var delete_context = httpx.Context.init(std.testing.allocator, std.testing.io, &delete_request);
    defer delete_context.deinit();
    var delete_response = try handler.executeSQL(&delete_context);
    defer delete_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), delete_response.status.code);
    const delete_output = try std.json.parseFromSlice(wire.SQLResponse, std.testing.allocator, delete_response.body.?, .{});
    defer delete_output.deinit();
    try std.testing.expectEqual(@as(i64, 1), delete_output.value.rows_affected);
    try std.testing.expectEqual(captures_before_delete + 1, fake.captures);
    try std.testing.expectEqual(attempts_before_delete + 1, fake.commit_attempts);
    try std.testing.expectEqual(commits_before_delete + 1, fake.commits);
    try std.testing.expect(fake.target_guard and fake.source_guard);
    fake.delete_case = false;
    // Each exact source shape must retain the same no-replay error contract
    // after its own candidate binding and image preparation, not only the
    // base MERGE statement's fault path.
    for ([_]struct { statement: []const u8, expression: bool = false, conditional: bool = false, returning: bool = false, insert_returning: bool = false, expression_predicate: bool = false, grouped_predicate: bool = false, cte: bool = false }{
        .{ .statement = sql },
        .{ .statement = conditional_sql, .conditional = true },
        .{ .statement = returning_sql, .returning = true },
        .{ .statement = expression_sql, .expression = true },
        .{ .statement = insert_returning_sql, .insert_returning = true },
        .{ .statement = predicate_sql[0], .expression_predicate = true },
        .{ .statement = predicate_sql[1], .grouped_predicate = true },
        .{ .statement = cte_sql, .cte = true },
    }) |case| {
        fake.expression_case = case.expression;
        fake.conditional_case = case.conditional;
        fake.returning_case = case.returning;
        fake.insert_returning_case = case.insert_returning;
        fake.expression_predicate_case = case.expression_predicate;
        fake.grouped_predicate_case = case.grouped_predicate;
        fake.cte_case = case.cte;
        const fault_body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = case.statement }, .{});
        defer std.testing.allocator.free(fault_body);
        for ([_]struct { conflict: bool, code: []const u8, retryable: bool }{
            .{ .conflict = true, .code = "40001", .retryable = true },
            .{ .conflict = false, .code = "40003", .retryable = false },
        }) |fault| {
            fake.source_conflict = fault.conflict;
            fake.unknown_commit = !fault.conflict;
            fake.target_guard = false;
            fake.source_guard = false;
            const captures_before_fault = fake.captures;
            const attempts_before_fault = fake.commit_attempts;
            const commits_before_fault = fake.commits;
            var fault_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql");
            defer fault_request.deinit();
            fault_request.body = fault_body;
            var fault_context = httpx.Context.init(std.testing.allocator, std.testing.io, &fault_request);
            defer fault_context.deinit();
            var fault_response = try handler.executeSQL(&fault_context);
            defer fault_response.deinit();
            try std.testing.expectEqual(@as(u16, 409), fault_response.status.code);
            const fault_diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, std.testing.allocator, fault_response.body.?, .{});
            defer fault_diagnostic.deinit();
            try std.testing.expectEqualStrings(fault.code, fault_diagnostic.value.code);
            try std.testing.expectEqual(@as(?bool, fault.retryable), fault_diagnostic.value.retryable);
            if (!fault.conflict) try std.testing.expectEqual(@as(usize, 32), fault_diagnostic.value.transaction_id.?.len);
            try std.testing.expectEqual(captures_before_fault + 1, fake.captures);
            try std.testing.expectEqual(attempts_before_fault + 1, fake.commit_attempts);
            try std.testing.expectEqual(commits_before_fault, fake.commits);
            try std.testing.expect(fake.target_guard and fake.source_guard);
        }
    }
    fake.source_conflict = false;
    fake.unknown_commit = false;
    fake.insert_returning_case = false;
    fake.expression_predicate_case = false;
    fake.grouped_predicate_case = false;
    fake.cte_case = false;
}

test "SQL document reads reject the relational stateless fallback before transport" {
    var identity: ?http_server.AuthenticatedIdentity = null;
    var adapter = Adapter{ .server = undefined, .identity = &identity, .context = .{} };
    try std.testing.expectError(error.UnsupportedSqlExecution, Adapter.scan(&adapter, std.testing.allocator, .{
        .id = 1,
        .physical_name = "docs",
        .schema_version = 1,
        .storage_mode = .document,
        .columns = &.{},
    }, .{ .fields = &.{}, .limit = 1 }));
    try std.testing.expectError(error.UnsupportedSqlExecution, Adapter.scan(&adapter, std.testing.allocator, .{
        .id = 1,
        .physical_name = "rows",
        .schema_version = 1,
        .columns = &.{},
    }, .{ .fields = &.{}, .include_primary_digest = true, .limit = 1 }));
}

test "SQL no-op mutations authorize read and write before consulting catalog" {
    var permission = [_]@import("../usermgr/mod.zig").Permission{.{ .resource = @constCast("*"), .resource_type = .table, .type = .write }};
    var identity: ?http_server.AuthenticatedIdentity = .{ .username = @constCast("writer"), .permissions = &permission };
    // Reaching server/catalog would be invalid: denied read-and-write bindings
    // must fail before metadata access, including contradictory/no-op writes.
    var adapter = Adapter{ .server = undefined, .identity = &identity, .context = .{} };
    const compiler = @import("../sql/compiler.zig");
    const runtime = @import("../sql/runtime.zig");
    for ([_][]const u8{ "DELETE FROM docs WHERE _id = NULL", "UPDATE docs SET name = 'x' WHERE _id = 'a' AND _id = 'b'" }) |statement| {
        var compiled = try compiler.compile(std.testing.allocator, statement, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.Forbidden, runtime.execute(std.testing.allocator, adapter.backend(), &compiled, &.{}, .{}));
    }
}

test "SQL unknown mutation keeps native reconciliation receipt without allocation" {
    const committed_id = mutationTransactionId("{\"status\":\"committed_pending\",\"transaction_id\":\"0123456789abcdef0123456789abcdef\"}");
    try std.testing.expectEqualStrings("0123456789abcdef0123456789abcdef", &committed_id.?);
    try std.testing.expectEqual(null, mutationTransactionId("{\"transaction_id\":\"invalid\"}"));
    const receipt = classifyMutationFailure(409, "{\"code\":\"transaction_outcome_unknown\",\"retryable\":false,\"transaction_id\":\"0123456789abcdef0123456789abcdef\"}");
    try std.testing.expectEqual(error.SqlMutationOutcomeUnknown, receipt.err);
    try std.testing.expectEqualStrings("0123456789abcdef0123456789abcdef", &receipt.transaction_id.?);
    try std.testing.expectEqual(error.DuplicateSqlRow, classifyMutationFailure(409, "{\"error\":\"UniqueConstraintViolation\"}").err);
    try std.testing.expectEqual(error.SqlMutationOutcomeUnknown, classifyMutationFailure(503, "write committed locally; standby durability acknowledgment pending").err);
    try std.testing.expectEqual(catalog.MutationOutcome.committed_pending, try committedMutationOutcome("{\"status\":\"committed_pending\"}"));
    try std.testing.expectEqual(catalog.MutationOutcome.committed_repair_required, try committedMutationOutcome("{\"status\":\"committed_repair_required\"}"));
    try std.testing.expectEqual(catalog.MutationOutcome.committed_graph_metric_materialization_rejected, try committedMutationOutcome("{\"status\":\"committed_repair_required\",\"failure\":{\"code\":\"graph_metric_materialization_rejected\"}}"));
    try std.testing.expectError(error.SqlMutationOutcomeUnknown, committedMutationOutcome("{\"status\":\"unknown\"}"));
}

test "SQL mutation classification preserves wrapped definite conflicts and constraints" {
    const cases = [_]struct { status: u16, body: []const u8, expected: anyerror }{
        .{ .status = 409, .body = "{\"error\":\"batch transaction conflicted\"}", .expected = error.SqlWriteConflict },
        .{ .status = 409, .body = "{\"error\":\"ForeignKeyReferenced\"}", .expected = error.ForeignKeyViolation },
        .{ .status = 400, .body = "{\"error\":\"RelationalCheckViolation\"}", .expected = error.RelationalCheckViolation },
        .{ .status = 413, .body = "{\"error\":\"RelationalIndexKeyTooLarge\"}", .expected = error.RelationalIndexKeyTooLarge },
        .{ .status = 409, .body = "{\"error\":\"standby is read-only\"}", .expected = error.HAReadOnlyStandby },
        .{ .status = 409, .body = "{\"code\":\"transaction_outcome_unknown\",\"error\":\"UniqueConstraintViolation\"}", .expected = error.SqlMutationOutcomeUnknown },
        .{ .status = 429, .body = "{\"code\":\"transaction_outcome_unknown\"}", .expected = error.SqlMutationOutcomeUnknown },
        .{ .status = 409, .body = "{\"status\":\"committed_pending\",\"error\":\"UniqueConstraintViolation\"}", .expected = error.SqlMutationOutcomeUnknown },
        .{ .status = 500, .body = "{\"error\":\"UniqueConstraintViolation\"}", .expected = error.SqlMutationOutcomeUnknown },
        .{ .status = 400, .body = "{malformed receipt", .expected = error.SqlMutationOutcomeUnknown },
    };
    for (cases) |case| try std.testing.expectEqual(case.expected, classifyMutationFailure(case.status, case.body).err);
}
