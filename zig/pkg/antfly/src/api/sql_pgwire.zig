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

//! Native pgwire adapter. No HTTP self-requests, durable SQL side catalog,
//! password retention, or automatic mutation replay. Listener registration is
//! deliberately separate from this authenticated engine boundary.
const std = @import("std");
const wire = @import("../pgwire/backend.zig");
const wire_values = @import("../pgwire/values.zig");
const storage_schema = @import("../storage/schema.zig");
const http = @import("http_server.zig");
const execution = @import("sql_execution.zig");
const compiler = @import("../sql/compiler.zig");
const describe_sql = @import("../sql/describe.zig");
const native = @import("../sql/runtime.zig");
const catalog = @import("../sql/catalog.zig");
const ast = @import("../sql/ast.zig");
const operation = @import("operation.zig");
const usermgr = @import("../usermgr/mod.zig");
const io_abi = @import("../runtime_io_abi.zig");
const Mac = std.crypto.auth.hmac.sha2.HmacSha256;
const credential_domain = "antfly.pgwire.password-session.v1";

/// Stable API-owned callback and listener storage. Shutdown joins all native
/// work before releasing the adapter, user manager, or backend runtime.
pub const Listener = struct {
    alloc: std.mem.Allocator,
    adapter: Adapter,
    server: @import("../pgwire/server.zig").Server,

    pub fn start(api: *http.ApiHttpServer, config: @import("../common/config.zig").Config.PgwireConfig) !*Listener {
        if (api.cfg.user_manager == null) return error.PgwireRequiresAuthentication;
        if (api.cfg.backend_runtime == null) return error.PgwireRequiresBackendRuntime;
        const io = api.sharedApiNetworkIo() orelse return error.PgwireRequiresBackendRuntime;
        const self = try api.owner_alloc.create(Listener);
        errdefer api.owner_alloc.destroy(self);
        self.alloc = api.owner_alloc;
        self.adapter = .{ .server = api };
        self.server = try @import("../pgwire/server.zig").start(self.alloc, .{
            .io = io,
            .backend = self.adapter.backend(),
            .bind_host = config.bind_host orelse "127.0.0.1",
            .bind_port = config.bind_port,
            .max_connections = config.max_connections,
            .allow_insecure_non_loopback = config.externally_protected_transport,
        });
        return self;
    }

    pub fn deinit(self: *Listener) void {
        self.server.deinit();
        self.alloc.destroy(self);
    }
};

test "SQL pgwire owner rejects unauthenticated and runtime-less startup" {
    var api: http.ApiHttpServer = undefined;
    api.cfg = .{};
    try std.testing.expectError(error.PgwireRequiresAuthentication, Listener.start(&api, .{ .enabled = true }));
    var manager: usermgr.UserManager = undefined;
    api.cfg.user_manager = &manager;
    try std.testing.expectError(error.PgwireRequiresBackendRuntime, Listener.start(&api, .{ .enabled = true }));
}

pub const Adapter = struct {
    server: *http.ApiHttpServer,

    pub fn backend(self: *Adapter) wire.Backend {
        return .{ .context = self, .vtable = &.{ .authenticate = authenticate, .describe = describe, .execute = execute, .disconnect = disconnect } };
    }

    fn authenticate(raw: *anyopaque, alloc: std.mem.Allocator, username: []const u8, password: []const u8) !wire.Identity {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        // A password callback is mandatory even if HTTP auth is disabled.
        // Neither a startup username nor database name creates authority.
        const manager = self.server.cfg.user_manager orelse return error.Unauthorized;
        if (username.len == 0 or username.len > 256 or password.len > 4096) return error.Unauthorized;
        const credential = try Credential.authenticate(alloc, manager, username, password);
        return .{ .context = credential, .release = Credential.release };
    }

    fn describe(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !wire.Description {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        var job = Job{ .adapter = self, .alloc = alloc, .credential = @ptrCast(@alignCast(identity.context)), .request = request, .kind = .describe };
        try self.dispatch(&job);
        return job.description.?;
    }

    fn execute(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !wire.Result {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        var job = Job{ .adapter = self, .alloc = alloc, .credential = @ptrCast(@alignCast(identity.context)), .request = request, .kind = .execute };
        try self.dispatch(&job);
        return job.result.?;
    }

    fn disconnect(raw: *anyopaque, identity: wire.Identity, session_id: ?[]const u8) void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const id = @import("distributed_txn.zig").parseTxnIdHex(session_id orelse return) catch return;
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        const runtime = self.server.cfg.backend_runtime orelse return;
        var io = runtime.io() orelse return;
        const network_io = self.server.sharedApiNetworkIo() orelse return;
        const Cleanup = struct {
            server: *http.ApiHttpServer,
            principal: []const u8,
            id: [16]u8,
            completion_io: std.Io,
            done: std.Io.Event = .unset,

            fn run(job: *@This()) void {
                defer job.done.set(job.completion_io);
                const access = job.server.txn_sessions.principalAccess(job.server.alloc, job.id, job.principal) catch return;
                if (access != .allowed) return;
                // Never erase a started distributed decision; durable session
                // maintenance retains it after this connection disappears.
                _ = job.server.txn_sessions.removeBeforeExecution(job.server.alloc, job.id);
            }
        };
        var job = Cleanup{ .server = self.server, .principal = if (std.mem.startsWith(u8, credential.principal, "basic:")) credential.username else credential.principal, .id = id, .completion_io = network_io };
        var future = io.concurrent(Cleanup.run, .{&job}) catch return;
        job.done.waitUncancelable(network_io);
        _ = future.await(io);
    }

    fn dispatch(self: *Adapter, job: *Job) !void {
        try validateRequest(job.request);
        if (self.server.cfg.user_manager != job.credential.manager) return error.Unauthorized;
        const runtime = self.server.cfg.backend_runtime orelse return error.SqlWriteCapacityUnavailable;
        var io = runtime.io() orelse return error.SqlWriteCapacityUnavailable;
        var future = io.concurrent(Job.run, .{job}) catch return error.SqlWriteCapacityUnavailable;
        // Keep borrowed request storage alive through a durable decision. The
        // listener sets cancellation/deadline on shutdown, native checkpoints
        // observe it, and this adapter joins rather than abandoning the job.
        job.done.waitUncancelable(job.request.io);
        _ = future.await(io);
        if (job.failure) |err| return err;
    }
};

const Credential = struct {
    manager: *usermgr.UserManager,
    username: []u8,
    principal: []u8,
    verifier_mac: [Mac.mac_length]u8,

    fn authenticate(alloc: std.mem.Allocator, manager: *usermgr.UserManager, username: []const u8, password: []const u8) !*Credential {
        // Snapshot only while locked; bcrypt must not serialize policy reads
        // and writes. Both ingress paths use UserManager's shared verifier.
        var user = snapshotUser(manager, username) catch return error.Unauthorized;
        defer user.deinit(manager.alloc);
        try usermgr.verifyPassword(user.password_hash, password);
        const self = try alloc.create(Credential);
        errdefer alloc.destroy(self);
        const name = try alloc.dupe(u8, username);
        errdefer alloc.free(name);
        const principal = try std.fmt.allocPrint(alloc, "basic:{s}", .{username});
        errdefer alloc.free(principal);
        var verifier_mac: [Mac.mac_length]u8 = undefined;
        Mac.create(&verifier_mac, credential_domain, user.password_hash);
        self.* = .{ .manager = manager, .username = name, .principal = principal, .verifier_mac = verifier_mac };
        // Detect rotation/deletion between snapshot, expensive verification,
        // and session publication without retaining either password or hash.
        try self.validate();
        return self;
    }

    fn validate(self: *const Credential) !void {
        const current = self.manager.destinationGrantMac(self.principal, credential_domain) catch return error.Unauthorized;
        if (!std.crypto.timing_safe.eql([Mac.mac_length]u8, current, self.verifier_mac)) return error.Unauthorized;
    }

    fn release(raw: *anyopaque, alloc: std.mem.Allocator) void {
        const self: *Credential = @ptrCast(@alignCast(raw));
        std.crypto.secureZero(u8, &self.verifier_mac);
        alloc.free(self.username);
        alloc.free(self.principal);
        alloc.destroy(self);
    }

    fn identity(self: *Credential, alloc: std.mem.Allocator) !http.AuthenticatedIdentity {
        try self.validate();
        var receiver = try self.manager.io_borrow.receive();
        const io = receiver.io();
        try self.manager.mutation_mutex.lock(io);
        // These snapshot methods do not take the mutation mutex internally.
        // Copy one coherent policy view without keeping a login-time grant or
        // row-policy cache across later statements.
        const result = blk: {
            defer self.manager.mutation_mutex.unlock(io);
            var user = try self.manager.getUser(self.username);
            defer user.deinit(self.manager.alloc);
            const permissions = try self.manager.getPermissionsForUser(self.username);
            defer http.freePermissions(self.manager.alloc, permissions);
            const filters = try self.manager.getRowFilters(self.username);
            defer http.freeRowFilters(self.manager.alloc, filters);
            const roles = try self.manager.getRolesForUser(self.username);
            defer {
                for (roles) |role| self.manager.alloc.free(role);
                self.manager.alloc.free(roles);
            }
            break :blk (try http.cloneCatalogIdentity(alloc, .{
                .username = user.username,
                .credential_principal = self.principal,
                .permissions = permissions,
                .row_filter = filters,
                .metadata_json = user.metadata_json,
                .roles = roles,
                .live_user_manager = self.manager,
            })).?;
        };
        var owned = result;
        errdefer owned.deinit(alloc);
        try self.validate();
        return owned;
    }
};

fn snapshotUser(manager: *usermgr.UserManager, username: []const u8) !usermgr.User {
    var receiver = try manager.io_borrow.receive();
    const io = receiver.io();
    try manager.mutation_mutex.lock(io);
    defer manager.mutation_mutex.unlock(io);
    return manager.getUser(username);
}

fn validateRequest(request: wire.Request) !void {
    try request.check();
    if (request.deadline.clock != .awake) return error.UnsupportedSqlExecution;
    if (request.limit == 0 or request.limit > 4096 or request.parameters.len > 1024) return error.InvalidSqlParameters;
}

const Job = struct {
    adapter: *Adapter,
    alloc: std.mem.Allocator,
    credential: *Credential,
    request: wire.Request,
    kind: enum { describe, execute },
    description: ?wire.Description = null,
    result: ?wire.Result = null,
    failure: ?anyerror = null,
    compile_diagnostic: compiler.Diagnostic = .{},
    compile_diagnostic_valid: bool = false,
    done: std.Io.Event = .unset,

    fn run(self: *Job) void {
        defer self.done.set(self.request.io);
        self.runInner() catch |err| {
            if (self.request.diagnostics) |diagnostic| if (diagnostic.code == null) {
                if (self.compile_diagnostic_valid) {
                    const safe = execution.diagnostic(err);
                    diagnostic.set(safe.code, self.compile_diagnostic.message, null, safe.retryable);
                } else {
                    var message_buffer: [256]u8 = undefined;
                    const safe = execution.diagnostic(err);
                    diagnostic.set(safe.code, execution.diagnosticMessage(err, &message_buffer), null, safe.retryable);
                }
            };
            self.failure = err;
        };
    }

    fn runInner(self: *Job) !void {
        try validateRequest(self.request);
        var preparation = self.adapter.server.sql_preparation_admission.tryAcquireLease() orelse return error.SqlWriteCapacityUnavailable;
        defer preparation.release();
        var lease = self.adapter.server.sqlPlanCache().acquire(self.adapter.server.sqlPlanCacheIo(), .{
            .statement = self.request.statement,
            .principal = self.credential.principal,
            .database = self.request.database orelse "default",
            .namespace = self.request.namespace orelse "public",
        }, &self.compile_diagnostic) catch |err| {
            self.compile_diagnostic_valid = err != error.SqlPlanCacheBusy and err != error.InvalidSqlPlanCacheConfig;
            return err;
        };
        defer lease.release(self.adapter.server.sqlPlanCacheIo());
        const compiled = lease.compiled();
        const write = self.kind == .execute and switch (compiled.statement) {
            .select => false,
            .insert, .update, .delete, .create_table, .drop_table, .catalog_ddl, .begin, .commit, .rollback, .savepoint, .rollback_to_savepoint, .release_savepoint => true,
        };
        const server = self.adapter.server;
        var admission = try server.acquireSqlExecution(write);
        defer admission.release();
        preparation.release();
        var identity: ?http.AuthenticatedIdentity = try self.credential.identity(server.alloc);
        defer identity.?.deinit(server.alloc);
        var authority = Authority{ .credential = self.credential, .identity = &identity, .request = self.request };
        const context = try authority.context();
        var native_adapter = execution.Adapter{
            .server = server,
            .identity = &identity,
            .context = context,
            .database = self.request.database orelse "default",
            .namespace = self.request.namespace orelse "public",
            .session_id = self.request.session_id,
        };
        var guarded = GuardedCatalog{
            .native = native_adapter.backend(),
            .authority = &authority,
            .revision = &native_adapter.revision,
            .expected_guard = self.request.binding_guard,
        };
        if (self.kind == .describe) {
            switch (compiled.statement) {
                .begin, .commit, .rollback, .savepoint, .rollback_to_savepoint, .release_savepoint => {
                    if (self.request.parameter_types.len != 0) return error.InvalidSqlParameters;
                    self.description = .{ .columns = &.{} };
                    return;
                },
                else => {},
            }
            const hints = try self.alloc.alloc(?ast.ColumnType, self.request.parameter_types.len);
            defer self.alloc.free(hints);
            for (self.request.parameter_types, hints) |kind, *hint| hint.* = switch (kind) {
                .unknown => null,
                inline else => |tag| @field(ast.ColumnType, @tagName(tag)),
            };
            var description = try describe_sql.describe(self.alloc, guarded.backend(), compiled, hints);
            defer description.deinit();
            const columns = try self.alloc.alloc(wire.Column, description.binding.columns.len);
            for (columns, description.binding.columns) |*out, column| out.* = .{ .name = try self.alloc.dupe(u8, column.name), .type = wireType(column.type) };
            const parameter_types = try self.alloc.alloc(wire.Type, description.binding.parameter_types.len);
            for (parameter_types, description.binding.parameter_types) |*out, kind| out.* = if (kind) |value| wireType(value) else .unknown;
            self.description = .{
                .columns = columns,
                .parameter_types = parameter_types,
                .binding_guard = if (description.binding.table) |table| try bindingGuard(self.alloc, native_adapter.revision orelse return error.InvalidSqlBackendResponse, table) else null,
            };
            return;
        }
        const parameters = try normalizeParameters(self.alloc, self.request.parameters, self.request.parameter_types);
        var result = native_adapter.execute(self.alloc, compiled, parameters, .{ .result_rows = self.request.limit, .page_rows = 4096 }, guarded.backend()) catch |err| {
            if (self.request.diagnostics) |diagnostic| diagnostic.transaction_status = @enumFromInt(@intFromEnum(native_adapter.transaction_status));
            if (err == error.SqlMutationOutcomeUnknown or err == error.SqlTransactionOutcomeUnknown or err == error.SessionLeaseLost) if (self.request.diagnostics) |diagnostic|
                diagnostic.set("40003", "transaction outcome is unknown; do not replay this statement", native_adapter.outcome_transaction_id, false);
            return err;
        };
        var transferred = false;
        defer if (!transferred) result.deinit();
        if (self.request.diagnostics) |diagnostic| diagnostic.transaction_status = @enumFromInt(@intFromEnum(native_adapter.transaction_status));
        errdefer if (native_adapter.result_session_id) |id| {
            const transaction_id = @import("distributed_txn.zig").parseTxnIdHex(&id) catch unreachable;
            server.txn_sessions.setSqlFailed(server.alloc, transaction_id, true) catch {};
            if (self.request.diagnostics) |diagnostic| diagnostic.transaction_status = .failed;
        };
        // After a durable commit, even acknowledgement-allocation failure must
        // preserve uncertainty instead of looking like a pre-admission abort.
        errdefer if (result.output.mutation_outcome != null) {
            if (self.request.diagnostics) |diagnostic|
                diagnostic.set("40003", "mutation committed but its acknowledgement could not be prepared; do not replay", native_adapter.outcome_transaction_id, false);
        };
        // Only datetime cells require representation adaptation. Mutate the
        // exclusively owned row cells, not a second copy of every result row.
        for (result.output.columns, 0..) |column, index| if (column.type == .datetime) {
            for (0..result.output.rows.len) |row_index| {
                const row = result.mutableRow(row_index);
                row[index] = try datetimeResult(self.alloc, row[index]);
            }
        };
        const columns = try self.alloc.alloc(wire.Column, result.output.columns.len);
        for (columns, result.output.columns) |*out, column| out.* = .{ .name = column.name, .type = wireType(column.type) };
        self.result = .{
            .columns = columns,
            .rows = result.output.rows,
            .sql_nulls = result.output.sql_nulls,
            .rows_affected = result.output.rows_affected,
            .command_tag = try commandTag(self.alloc, result.output.command_tag, result.output.rows.len, result.output.rows_affected),
            .mutation_outcome = if (result.output.mutation_outcome) |outcome| switch (outcome) {
                inline else => |kind| @field(wire.MutationOutcome, @tagName(kind)),
            } else null,
            .transaction_id = native_adapter.outcome_transaction_id,
            .ddl_receipt_json = if (result.output.ddl_receipt) |receipt| try std.json.Stringify.valueAlloc(self.alloc, receipt, .{ .emit_null_optional_fields = false }) else null,
            .session_id = if (native_adapter.result_session_id) |id| try self.alloc.dupe(u8, &id) else null,
            .transaction_status = @enumFromInt(@intFromEnum(native_adapter.transaction_status)),
            .owner = .{ .context = result.state, .release = releaseResult },
        };
        transferred = true;
    }
};

fn normalizeParameters(alloc: std.mem.Allocator, input: []const std.json.Value, types: []const wire.Type) ![]const std.json.Value {
    if (input.len != types.len) return error.InvalidSqlParameters;
    var normalized: ?[]std.json.Value = null;
    for (input, types, 0..) |value, kind, index| {
        if (kind != .datetime or value == .null or value == .string) continue;
        if (normalized == null) normalized = try alloc.dupe(std.json.Value, input);
        normalized.?[index] = .{ .string = try storage_schema.formatDateTimeNsAlloc(alloc, try wire_values.timestampNanos(value)) };
    }
    return normalized orelse input;
}

fn datetimeResult(alloc: std.mem.Allocator, value: std.json.Value) !std.json.Value {
    if (value == .null) return value;
    const nanos = if (value == .string)
        storage_schema.parseDateTimeToNs(value.string) orelse return error.InvalidSqlBackendResponse
    else
        try wire_values.timestampNanos(value);
    return wire_values.timestampValue(alloc, nanos);
}

fn wireType(kind: ast.ColumnType) wire.Type {
    return switch (kind) {
        inline else => |tag| @field(wire.Type, @tagName(tag)),
    };
}

fn commandTag(alloc: std.mem.Allocator, command: []const u8, rows: usize, affected: u64) ![]const u8 {
    if (std.mem.eql(u8, command, "DDL PENDING")) return alloc.dupe(u8, command);
    if (std.mem.eql(u8, command, "SELECT")) return std.fmt.allocPrint(alloc, "SELECT {d}", .{rows});
    if (std.mem.eql(u8, command, "INSERT")) return std.fmt.allocPrint(alloc, "INSERT 0 {d}", .{affected});
    if (std.mem.eql(u8, command, "UPDATE") or std.mem.eql(u8, command, "DELETE")) return std.fmt.allocPrint(alloc, "{s} {d}", .{ command, affected });
    if (std.mem.startsWith(u8, command, "CREATE ") or std.mem.startsWith(u8, command, "DROP ") or std.mem.startsWith(u8, command, "ALTER ")) return alloc.dupe(u8, command);
    inline for (.{ "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE" }) |tag| if (std.mem.eql(u8, command, tag)) return alloc.dupe(u8, command);
    return error.UnsupportedSqlExecution;
}

fn releaseResult(raw: *anyopaque) void {
    var result = native.Result{ .state = @ptrCast(@alignCast(raw)), .output = undefined };
    result.deinit();
}

const Authority = struct {
    credential: *Credential,
    identity: *?http.AuthenticatedIdentity,
    request: wire.Request,

    fn checkpoint(self: *Authority) !void {
        try self.request.check();
    }

    fn context(self: *Authority) !operation.RequestContext {
        try self.checkpoint();
        try self.credential.validate();
        return .{
            .deadline_ns = std.math.cast(u64, self.request.deadline.raw.nanoseconds) orelse return error.DeadlineExceeded,
            .deadline_io = io_abi.Borrow.init(&self.request.io),
            .fanout_io = io_abi.Borrow.init(&self.request.io),
            .cancellation = operation.CancellationToken.fromAtomic(self.request.cancel_requested),
            .principal = .{ .kind = .user, .subject = self.identity.*.?.username },
            .destination_authorization_principal = self.credential.principal,
            .table_write_authorization = .{ .ptr = self, .allows = allowsWrite },
        };
    }

    fn allowsWrite(raw: *const anyopaque, table: []const u8) bool {
        const self: *const Authority = @ptrCast(@alignCast(raw));
        self.credential.validate() catch return false;
        return http.tablePermissionCurrentlyAllowed(self.identity.*, table, .write) catch false;
    }
};

const GuardedCatalog = struct {
    native: catalog.Backend,
    authority: *Authority,
    revision: *const ?u64,
    expected_guard: ?[]const u8,
    fn backend(self: *GuardedCatalog) catalog.Backend {
        return .{ .ptr = self, .pinned_statement_snapshot = self.native.pinned_statement_snapshot, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = openScan, .mutate = mutate, .checkpoint = checkpoint, .ddl = ddl } };
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try self.authority.checkpoint();
        try self.native.vtable.checkpoint(self.native.ptr);
    }
    fn resolve(raw: *anyopaque, alloc: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        try self.authority.credential.validate();
        const table = try self.native.vtable.resolve(self.native.ptr, alloc, name, action);
        if (self.expected_guard) |guard| try verifyBindingGuard(guard, self.revision.* orelse return error.InvalidSqlBackendResponse, table);
        return table;
    }
    fn scan(raw: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !catalog.Page {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try self.checkRead(table.physical_name);
        return self.native.vtable.scan(self.native.ptr, alloc, table, request);
    }
    fn checkRead(self: *GuardedCatalog, table: []const u8) !void {
        try checkpoint(self);
        try self.authority.credential.validate();
        if (!(try http.tablePermissionCurrentlyAllowed(self.authority.identity.*, table, .read))) return error.Forbidden;
    }
    const ReadCursor = struct {
        alloc: std.mem.Allocator,
        guard: *GuardedCatalog,
        table: []u8,
        inner: catalog.Cursor,

        fn next(raw: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.guard.checkRead(self.table);
            return self.inner.next(self.inner.ptr, alloc, limit);
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inner.close(self.inner.ptr);
            self.alloc.free(self.table);
            self.alloc.destroy(self);
        }
    };
    fn openScan(raw: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try self.checkRead(table.physical_name);
        const open = self.native.vtable.open_scan orelse return null;
        const inner = (try open(self.native.ptr, alloc, table, request)) orelse return null;
        errdefer inner.close(inner.ptr);
        const name = try alloc.dupe(u8, table.physical_name);
        errdefer alloc.free(name);
        const cursor = try alloc.create(ReadCursor);
        cursor.* = .{ .alloc = alloc, .guard = self, .table = name, .inner = inner };
        // Native execution closes this cursor before returning its materialized
        // result. Portals never retain the stack-owned guard or authority.
        return .{ .ptr = cursor, .next = ReadCursor.next, .close = ReadCursor.close };
    }
    fn mutate(raw: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        if (!Authority.allowsWrite(self.authority, table.physical_name)) return error.Forbidden;
        return self.native.vtable.mutate(self.native.ptr, alloc, table, input);
    }

    fn ddl(raw: *anyopaque, alloc: std.mem.Allocator, input: catalog.Ddl) !catalog.DdlOutcome {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        const dispatch_ddl = self.native.vtable.ddl orelse return error.UnsupportedSqlExecution;
        // Refresh current grants before native DDL authorizes resource, rename
        // destination and tablespace. Prepared metadata never grants authority.
        var current = try self.authority.credential.identity(alloc);
        defer current.deinit(alloc);
        const prior = self.authority.identity.*;
        self.authority.identity.* = current;
        defer self.authority.identity.* = prior;
        return dispatch_ddl(self.native.ptr, alloc, input);
    }
};

fn bindingGuard(alloc: std.mem.Allocator, revision: u64, table: catalog.Table) ![]const u8 {
    const bytes = try alloc.alloc(u8, 20 + table.physical_name.len);
    std.mem.writeInt(u64, bytes[0..8], revision, .little);
    std.mem.writeInt(u64, bytes[8..16], table.id, .little);
    std.mem.writeInt(u32, bytes[16..20], table.schema_version, .little);
    @memcpy(bytes[20..], table.physical_name);
    return bytes;
}

fn verifyBindingGuard(guard: []const u8, revision: u64, table: catalog.Table) !void {
    if (guard.len < 20 or
        std.mem.readInt(u64, guard[0..8], .little) != revision or
        std.mem.readInt(u64, guard[8..16], .little) != table.id or
        std.mem.readInt(u32, guard[16..20], .little) != table.schema_version or
        !std.mem.eql(u8, guard[20..], table.physical_name)) return error.CatalogGenerationChanged;
}

test "SQL pgwire prepared identity rejects same shaped table replacement before execution" {
    const table = catalog.Table{ .id = 42, .physical_name = "physical_docs", .schema_version = 1, .columns = &.{} };
    const guard = try bindingGuard(std.testing.allocator, 7, table);
    defer std.testing.allocator.free(guard);
    try verifyBindingGuard(guard, 7, table);
    var replacement = table;
    replacement.id = 43;
    try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard(guard, 7, replacement));
    replacement = table;
    replacement.schema_version = 2;
    try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard(guard, 7, replacement));
    try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard(guard, 8, table));
    try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard("malformed", 7, table));
}

test "SQL pgwire credential snapshot observes policy revocation and password rotation" {
    const alloc = std.testing.allocator;
    const casbin = @import("antfly_casbin");
    var store = usermgr.MemoryStore.init(alloc);
    defer store.deinit();
    var policies = casbin.MemoryAdapter.init(alloc);
    defer policies.deinit();
    var manager = try usermgr.UserManager.init(alloc, store.iface(), try usermgr.initDefaultEnforcer(alloc, policies.iface()));
    defer manager.deinit();
    var read = try usermgr.Permission.initOwned(alloc, .table, "docs", .read);
    defer read.deinit(alloc);
    var user = try manager.createUser("alice", "secret", &.{read});
    defer user.deinit(alloc);
    const SnapshotAllocation = struct {
        fn check(failing: std.mem.Allocator, original: *usermgr.UserManager) !void {
            // Read-only copy shares map data but routes only snapshot ownership
            // through the failing allocator. Never deinitialize the borrowed maps.
            var borrowed = original.*;
            borrowed.alloc = failing;
            var snapshot = try borrowed.getUser("alice");
            defer snapshot.deinit(failing);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, SnapshotAllocation.check, .{&manager});
    try std.testing.expectError(error.InvalidPassword, Credential.authenticate(alloc, &manager, "alice", "wrong"));
    const credential = try Credential.authenticate(alloc, &manager, "alice", "secret");
    defer Credential.release(credential, alloc);
    var admitted = try credential.identity(alloc);
    defer admitted.deinit(alloc);
    try std.testing.expect(try http.tablePermissionCurrentlyAllowed(admitted, "docs", .read));
    const NativeCursor = struct {
        pages: usize = 0,
        closes: usize = 0,
        fn open(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
            return .{ .ptr = ptr, .next = next, .close = close };
        }
        fn next(ptr: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.pages += 1;
            return .{ .rows = &.{}, .after = "next" };
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closes += 1;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var native_cursor: NativeCursor = .{};
    var identity: ?http.AuthenticatedIdentity = admitted;
    var cancellation: std.atomic.Value(bool) = .init(false);
    var authority = Authority{ .credential = credential, .identity = &identity, .request = .{
        .statement = "SELECT * FROM docs",
        .limit = 1,
        .io = std.testing.io,
        .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } },
        .cancel_requested = &cancellation,
    } };
    const revision: ?u64 = 1;
    var guarded: GuardedCatalog = .{
        .native = .{ .ptr = &native_cursor, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .open_scan = NativeCursor.open, .checkpoint = NativeCursor.checkpoint } },
        .authority = &authority,
        .revision = &revision,
        .expected_guard = null,
    };
    const backend = guarded.backend();
    const cursor = (try backend.vtable.open_scan.?(backend.ptr, alloc, .{ .id = 1, .physical_name = "docs", .schema_version = 1, .columns = &.{} }, .{ .fields = &.{}, .limit = 1 })).?;
    var cursor_closed = false;
    defer if (!cursor_closed) cursor.close(cursor.ptr);
    const page = try cursor.next(cursor.ptr, alloc, 1);
    page.deinit();
    try std.testing.expectEqual(@as(usize, 1), native_cursor.pages);
    try manager.removePermissionFromUser("alice", "docs", .table);
    try std.testing.expectError(error.Forbidden, cursor.next(cursor.ptr, alloc, 1));
    try std.testing.expectEqual(@as(usize, 1), native_cursor.pages);
    cursor.close(cursor.ptr);
    cursor_closed = true;
    try std.testing.expectEqual(@as(usize, 1), native_cursor.closes);
    try std.testing.expect(!(try http.tablePermissionCurrentlyAllowed(admitted, "docs", .read)));
    var fresh = try credential.identity(alloc);
    defer fresh.deinit(alloc);
    try std.testing.expect(!(try http.tablePermissionCurrentlyAllowed(fresh, "docs", .read)));
    try manager.updatePassword("alice", "new-secret");
    try std.testing.expectError(error.Unauthorized, credential.validate());
    try std.testing.expectError(error.Unauthorized, credential.identity(alloc));
}

test "SQL pgwire native adapter admits sessions for principal scoped native validation" {
    // Force semantic compilation of every production callback without needing
    // a listener or a live distributed backend in this owner-layer test.
    var adapter = Adapter{ .server = undefined };
    const callbacks = adapter.backend();
    try std.testing.expect(callbacks.context == @as(*anyopaque, @ptrCast(&adapter)));
    var cancelled = std.atomic.Value(bool).init(false);
    const request = wire.Request{
        .statement = "SELECT * FROM docs",
        .limit = 4096,
        .session_id = "untrusted-session",
        .io = std.testing.io,
        .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } },
        .cancel_requested = &cancelled,
    };
    // Session identity is checked by the durable native owner, not rejected by
    // ingress merely because this request carries a transaction id.
    try validateRequest(request);
}

test "SQL pgwire command tags carry exact PostgreSQL counts" {
    const cases = [_]struct { command: []const u8, expected: []const u8 }{
        .{ .command = "SELECT", .expected = "SELECT 3" },
        .{ .command = "INSERT", .expected = "INSERT 0 7" },
        .{ .command = "UPDATE", .expected = "UPDATE 7" },
        .{ .command = "DELETE", .expected = "DELETE 7" },
    };
    for (cases) |case| {
        const tag = try commandTag(std.testing.allocator, case.command, 3, 7);
        defer std.testing.allocator.free(tag);
        try std.testing.expectEqualStrings(case.expected, tag);
    }
}

test "SQL pgwire binary datetime parameters cross native coercion without precision loss" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    // Native storage accepts unsigned nanoseconds; the high case must not
    // narrow through i64 even though the PostgreSQL wire uses signed micros.
    for ([_]u64{ 0, 946684800000000000, 18000000000123456000, std.math.maxInt(u64) / 1000 * 1000 }) |nanos| {
        const wire_value = try wire_values.timestampValue(alloc, nanos);
        const bytes = try wire_values.encode(alloc, .datetime, 1, wire_value);
        const decoded = try wire_values.decode(alloc, 1184, 1, bytes);
        const parameters = try normalizeParameters(alloc, &.{decoded}, &.{.datetime});
        const coerced = try describe_sql.coerce(parameters[0], .datetime);
        try std.testing.expect(coerced == .string);
        try std.testing.expectEqual(@as(?u64, nanos), storage_schema.parseDateTimeToNs(coerced.string));
        // The native SELECT representation is ISO; the adapter normalizes just
        // that owned cell and then produces the exact original binary micros.
        const returned = try datetimeResult(alloc, coerced);
        try std.testing.expectEqualSlices(u8, bytes, try wire_values.encode(alloc, .datetime, 1, returned));
        const text = try wire_values.encode(alloc, .datetime, 0, returned);
        try std.testing.expectEqual(@as(?u64, nanos), storage_schema.parseDateTimeToNs(text));
    }
    const text_value = std.json.Value{ .string = "2000-01-01T01:00:00+01:00" };
    const text_parameters = try normalizeParameters(alloc, &.{ text_value, .null }, &.{ .datetime, .datetime });
    try std.testing.expectEqualStrings(text_value.string, text_parameters[0].string);
    try std.testing.expect(text_parameters[1] == .null);
    const returned = try datetimeResult(alloc, text_value);
    try std.testing.expectEqual(@as(i64, 0), std.mem.readInt(i64, (try wire_values.encode(alloc, .datetime, 1, returned))[0..8], .big));
    const too_precise = try datetimeResult(alloc, .{ .string = "2000-01-01T00:00:00.000000001Z" });
    try std.testing.expectError(error.UnsupportedResultPrecision, wire_values.encode(alloc, .datetime, 1, too_precise));
    try std.testing.expectError(error.UnsupportedResultPrecision, wire_values.encode(alloc, .datetime, 0, too_precise));
    try std.testing.expectError(error.InvalidSqlBackendResponse, datetimeResult(alloc, .{ .string = "1969-01-01T00:00:00Z" }));
}
