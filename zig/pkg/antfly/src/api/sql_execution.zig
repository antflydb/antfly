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
        var session = sessions.Session{ .owner = owner.owner(), .scope = .{ .principal = http_server.transactionPrincipal(self.identity.*) orelse "", .database = self.database, .namespace = self.namespace } };
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
        if (session.transaction_id != null) {
            const transaction = try session.statement(switch (compiled.statement) {
                .select => false,
                else => true,
            });
            // DDL cannot bypass the native transaction's atomicity boundary.
            if (@import("../sql/ddl_runtime.zig").accepts(compiled.statement)) return error.UnsupportedSqlExecution;
            const id = session.transaction_id.?;
            const execution_lease = self.server.txn_sessions.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
            defer execution_lease.release();
            _ = try session.statement(switch (compiled.statement) {
                .select => false,
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
            self.inserting = compiled.statement == .insert and compiled.statement.insert.conflict == null;
            defer {
                self.active_transaction = null;
                self.staged = null;
                self.range_reads = null;
                self.ranges_staged = false;
                self.inserting = false;
            }
            var result = try @import("../sql/runtime.zig").execute(alloc, guarded_backend orelse self.backend(), compiled, parameters, limits);
            errdefer result.deinit();
            if (!self.ranges_staged and read_guards.tables.len != 0) {
                _ = (try self.server.txn_sessions.stage(self.server.alloc, id, &read_guards)) orelse return error.SqlTransactionNotActive;
            }
            result.output.mutation_outcome = null;
            return result;
        }
        return @import("../sql/runtime.zig").execute(alloc, guarded_backend orelse self.backend(), compiled, parameters, limits);
    }

    pub fn backend(self: *Adapter) catalog.Backend {
        return .{ .ptr = self, .predicate_only_mutations = true, .vtable = &.{ .resolve_conflict_owners = resolveConflictOwners, .generate_row_id = generateRowId, .resolve = resolve, .scan = scan, .open_scan = openScan, .open_statement = openStatement, .mutate = mutate, .prepare_mutations = prepareMutations, .ddl = ddl, .checkpoint = checkpoint } };
    }

    fn generateRowId(ptr: *anyopaque, alloc: std.mem.Allocator) ![]const u8 {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        try self.context.ensureActive();
        return @import("../storage/row_identity.zig").generate(alloc, self.server.sharedApiIo() orelse return error.UnsupportedSqlExecution);
    }

    fn resolveConflictOwners(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, columns: []const []const u8, mutations: []const catalog.Mutation) ![]const catalog.ConflictOwner {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        try self.verify(alloc, table);
        const integrity = @import("relational_integrity_commit.zig");
        var snapshot = (try self.server.source.adminSnapshot()) orelse return error.IntegrityCatalogUnavailable;
        defer self.server.source.freeAdminSnapshot(&snapshot);
        const writes = try @import("../sql/mutation_images.zig").writes(db_types.BatchWrite, alloc, mutations);
        const previous = if (self.staged) |staged| try staged.distributedTables(alloc) else &.{};
        const owners = try integrity.resolveConflictOwners(alloc, self.server.table_reads orelse return error.UnsupportedSqlExecution, snapshot.tables, snapshot.ranges, table.physical_name, table.schema_version, columns, writes, previous, self.context);
        try self.verify(alloc, table);
        const result = try alloc.alloc(catalog.ConflictOwner, owners.len);
        for (owners, result) |*owner, *out| out.* = .{ .key = owner.key, .identity = owner.identity, .guard = owner };
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
                    .auto_index = request.primary_key == null and !request.primary_order,
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
        for (requests, scans) |request, *input| {
            var query_request = request.request;
            if (self.staged != null) query_request.primary_order = true;
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
        errdefer if (self.staged != null) for (cursors[0..wrapped]) |cursor| cursor.close(cursor.ptr);
        for (requests, native.views, wrappers, cursors) |request, view, *wrapper, *cursor| {
            try self.verify(temporary, request.table);
            if (self.range_reads) |observed| {
                const proofs = try native.rangeProofs(temporary, wrapped);
                if (proofs.len == 0) return error.InvalidSqlBackendResponse;
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
            if (self.staged) |staged| {
                const row_filter = try http_server.resolveEffectiveRowFilterJson(temporary, self.identity.*, request.table.physical_name);
                var ordered = request.request;
                ordered.primary_order = true;
                cursor.* = try @import("sql_session_overlay.zig").open(alloc, cursor.*, staged, request.table, ordered, row_filter);
            }
            wrapped += 1;
        }
        retained.* = .{ .alloc = alloc, .native = native, .wrappers = wrappers, .cursors = cursors, .overlays = self.staged != null };
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
            const normalized = if (writes.items.len != 0 and table.storage_mode == .relational) blk: {
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

test "SQL API guarded sessions retain reads across savepoints and commit guards without writes" {
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
        emit_row: bool = false,
        expect_delete: bool = false,
        views: [1]View = undefined,
        records: [1]@import("../common/topology_records.zig").TableRecord = .{.{ .table_id = 3, .name = "physical", .schema_json = schema }},
        fn status(_: *anyopaque) !metadata.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn resolve(_: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, _: system_catalog.Call) ![]u8 {
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
            try std.testing.expectEqual(@as(usize, 1), scans.len);
            try std.testing.expect(scans[0].opts.include_range_proofs);
            if (!self.active) return error.SqlRangeTrackingRequired;
            self.views[0] = .{ .ptr = ptr, .vtable = &.{ .next = next, .close = close } };
            return .{ .ptr = ptr, .views = &self.views, .vtable = &.{ .close = close, .range_proofs = proofs } };
        }
        fn proofs(ptr: *anyopaque, alloc: std.mem.Allocator, _: usize) ![]reads.RelationalStatementRead.OwnerRangeProof {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const observations = try alloc.dupe(@import("range_read_guards.zig").Proof, &.{.{ .bucket = 98, .generation = self.generation }});
            return alloc.dupe(reads.RelationalStatementRead.OwnerRangeProof, &.{.{ .fence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 2, .table_id = 3, .topology_epoch = 4, .route = .{ .group_id = 5, .range_id = 6, .identity_namespace = .{ .table_id = 3, .shard_id = 5, .range_id = 6 } } }, .proofs = observations }});
        }
        fn next(ptr: *anyopaque, alloc: std.mem.Allocator, _: u32) !View.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.pages += 1;
            if (self.emit_row) {
                var arena = std.heap.ArenaAllocator.init(alloc);
                errdefer arena.deinit();
                const rows = try arena.allocator().alloc(View.Row, 1);
                rows[0] = .{ .id = "a", .version = 42, .schema_version = 7, .value = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"n\":1}", .{}), .expected_content_digest = @splat(9) };
                return .{ .arena = arena, .rows = rows, .after = null };
            }
            return .{ .arena = std.heap.ArenaAllocator.init(alloc), .rows = &.{}, .after = null };
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closes += 1;
        }
        fn commit(ptr: *anyopaque, _: std.mem.Allocator, _: db_types.TxnId, _: u64, tables: []const contract.TableCommitRequest, _: db_types.SyncLevel) !?contract.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), tables.len);
            try std.testing.expectEqualStrings("physical", tables[0].table_name);
            try std.testing.expectEqual(@as(usize, 0), tables[0].writes.len);
            try std.testing.expectEqual(@as(usize, if (self.expect_delete) 1 else 0), tables[0].deletes.len);
            if (self.expect_delete) {
                try std.testing.expectEqualStrings("a", tables[0].deletes[0]);
                try std.testing.expectEqual(@as(usize, 1), tables[0].predicates.len);
                try std.testing.expectEqual(@as(u64, 42), tables[0].predicates[0].expected_version);
                try std.testing.expectEqual(@as(?[32]u8, @splat(9)), tables[0].predicates[0].expected_content_digest);
            }
            try std.testing.expectEqual(@as(usize, 1), tables[0].range_guards.len);
            try std.testing.expectEqual(@as(?u64, 1), tables[0].range_guards[0].proofs[0].generation);
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
    var server = http_server.ApiHttpServer.init(std.testing.allocator, .{}, .{ .ptr = &fake, .vtable = &.{ .status = Fake.status, .system_catalog = Fake.resolve, .supports_query_definitions = true, .admin_snapshot = Fake.snapshot, .free_admin_snapshot = Fake.freeSnapshot } }, .{ .ptr = &fake, .supports_sql_range_guards = true, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .open_relational_statement = Fake.open } }, .{ .ptr = &fake, .supports_sql_range_guards = true, .vtable = &.{ .batch = undefined, .activate_range_tracking = Fake.activate, .commit_transaction_with_id = Fake.commit } });
    defer server.deinit();
    var identity: ?http_server.AuthenticatedIdentity = null;
    var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{} };
    // A provider flag is necessary; merely having callbacks is insufficient.
    server.table_writes.?.supports_sql_range_guards = false;
    try std.testing.expectError(error.UnsupportedSqlExecution, Fake.run(&adapter, "BEGIN ISOLATION LEVEL SERIALIZABLE"));
    server.table_writes.?.supports_sql_range_guards = true;
    try Fake.run(&adapter, "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY");
    var session = adapter.result_session_id.?;
    adapter.session_id = &session;
    try Fake.run(&adapter, "SAVEPOINT before_read");
    try Fake.run(&adapter, "SELECT n FROM docs");
    try Fake.run(&adapter, "ROLLBACK TO before_read");
    // Read observations cannot be erased by rolling back a savepoint.
    try Fake.run(&adapter, "COMMIT");
    try std.testing.expectEqual(@as(usize, 1), fake.commits);
    try std.testing.expectEqual(@as(usize, 1), fake.activations);
    adapter.session_id = null;
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
