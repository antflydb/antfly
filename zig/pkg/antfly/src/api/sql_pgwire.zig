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
        return .{ .context = self, .vtable = &.{ .authenticate = authenticate, .describe = describe, .execute = execute, .validate_namespace = validateNamespace, .evaluate_parameters = evaluateParameters, .fail_transaction = failTransaction, .open_stream = openStream, .disconnect = disconnect } };
    }

    fn failTransaction(raw: *anyopaque, identity: wire.Identity, request: wire.Request) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        const encoded = request.session_id orelse return error.SqlTransactionNotActive;
        const id = try @import("distributed_txn.zig").parseTxnIdHex(encoded);
        if (try self.server.txn_sessions.principalAccess(self.server.alloc, id, credential.sessionPrincipal()) != .allowed) return error.SqlTransactionNotActive;
        var state = (try self.server.txn_sessions.getSqlState(self.server.alloc, id)) orelse return error.SqlTransactionNotActive;
        defer state.deinit(self.server.alloc);
        if (state.terminal != null or !std.mem.eql(u8, state.metadata.database, request.database orelse "default") or !std.mem.eql(u8, state.metadata.namespace, request.session_namespace orelse request.namespace orelse "public")) return error.SqlTransactionNotActive;
        try self.server.txn_sessions.setSqlFailed(self.server.alloc, id, true);
    }

    fn evaluateParameters(_: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request, expressions: []const []const u8) ![]const std.json.Value {
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        try credential.validate();
        return evaluateScalarParameters(alloc, request, expressions);
    }

    fn evaluateScalarParameters(alloc: std.mem.Allocator, request: wire.Request, expressions: []const []const u8) ![]const std.json.Value {
        if (expressions.len != request.parameter_types.len) return error.InvalidSqlParameters;
        const scalar = @import("../sql/scalar.zig");
        const result = try alloc.alloc(std.json.Value, expressions.len);
        for (expressions, request.parameter_types, result) |expression, kind, *value| {
            try request.check();
            var compiled = try compiler.compileScalar(alloc, expression, .{});
            defer compiled.deinit();
            if (compiled.parameter_count != 0) return error.InvalidSqlParameters;
            const expected: ?ast.ColumnType = switch (kind) {
                .unknown => null,
                inline else => |tag| @field(ast.ColumnType, @tagName(tag)),
            };
            // Empty binding environment forbids table reads/correlated names;
            // the scalar compiler rejects subqueries and statement commands.
            var program = try scalar.bindExpected(alloc, compiled.expression, &.{}, &.{}, expected, .{});
            defer program.deinit();
            const evaluated = try program.evaluate(alloc, &.{}, &.{}, .{});
            value.* = if (kind == .json and !evaluated.sql_null)
                .{ .string = try std.json.Stringify.valueAlloc(alloc, evaluated.value, .{}) }
            else
                try native.clone(alloc, evaluated.value);
        }
        try request.check();
        return result;
    }

    fn openStream(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !?wire.ReadStream {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        var job = StreamJob{ .adapter = self, .alloc = alloc, .credential = @ptrCast(@alignCast(identity.context)), .request = request };
        try job.dispatch();
        return job.opened;
    }

    fn validateNamespace(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        var job = StreamJob{ .adapter = self, .alloc = alloc, .credential = @ptrCast(@alignCast(identity.context)), .request = request, .namespace_only = true };
        try job.dispatch();
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
        var job = Cleanup{ .server = self.server, .principal = credential.sessionPrincipal(), .id = id, .completion_io = network_io };
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

const Pull = @import("../sql/read_stream.zig");

/// The portal owns this entire capsule. No cursor retains Job.runInner's stack
/// identity, authority, schema binding or temporary request context.
const OwnedRead = struct {
    alloc: std.mem.Allocator,
    adapter: *Adapter,
    arena: std.heap.ArenaAllocator,
    identity: ?http.AuthenticatedIdentity,
    authority: Authority,
    native_adapter: execution.Adapter,
    session_id: ?[]u8 = null,
    staged: @import("transactions.zig").OwnedTransactionCommitRequest = .{},
    range_guards: @import("transactions.zig").OwnedTransactionCommitRequest = .{},
    guarded: GuardedCatalog,
    plan: @import("../sql/plan_cache.zig").Lease,
    admission: http.RequestAdmission.Lease,
    stream: *Pull.Stream,
    columns: []const wire.Column,
    policies: []const Policy,
    detached: bool = false,
    const Policy = struct { table: []const u8, filter: ?[]const u8, name: ?ast.Name = null, id: u64 = 0, schema_version: u32 = 0 };

    fn open(adapter: *Adapter, alloc: std.mem.Allocator, credential: *Credential, request: wire.Request) !?wire.ReadStream {
        const server = adapter.server;
        var preparation = server.sql_preparation_admission.tryAcquireLease() orelse return error.SqlWriteCapacityUnavailable;
        defer preparation.release();
        var diagnostic: compiler.Diagnostic = .{};
        var plan = try server.sqlPlanCache().acquire(server.sqlPlanCacheIo(), .{ .statement = request.statement, .principal = credential.principal, .database = request.database orelse "default", .namespace = request.namespace orelse "public" }, &diagnostic);
        var moved = false;
        defer if (!moved) plan.release(server.sqlPlanCacheIo());
        if (plan.compiled().statement != .select) return null;
        var admission = try server.acquireSqlExecution(false);
        defer if (!moved) admission.release();
        const self = try alloc.create(OwnedRead);
        errdefer alloc.destroy(self);
        self.alloc = alloc;
        self.session_id = null;
        self.staged = .{};
        self.range_guards = .{};
        self.detached = false;
        self.adapter = adapter;
        self.arena = std.heap.ArenaAllocator.init(alloc);
        errdefer self.arena.deinit();
        errdefer self.staged.deinit(alloc);
        errdefer self.range_guards.deinit(server.alloc);
        const arena = self.arena.allocator();
        if (request.session_id) |encoded| self.session_id = try server.alloc.dupe(u8, encoded);
        errdefer if (self.session_id) |id| server.alloc.free(id);
        // Native catalog resolution extends this identity with aliases using
        // server.alloc; every allocation in the mutable identity must share it.
        self.identity = try credential.identity(server.alloc);
        errdefer self.identity.?.deinit(server.alloc);
        // Statement, parameter and binding-guard data are portal-owned. Keep a
        // private request capsule so I/O borrows point to a stable address.
        self.authority = .{ .credential = credential, .identity = &self.identity, .request = request };
        self.authority.request.statement = try arena.dupe(u8, request.statement);
        self.authority.request.database = try arena.dupe(u8, request.database orelse "default");
        self.authority.request.namespace = try arena.dupe(u8, request.namespace orelse "public");
        self.authority.request.session_namespace = if (request.session_namespace) |scope| try arena.dupe(u8, scope) else null;
        self.authority.request.binding_guard = if (request.binding_guard) |guard| try arena.dupe(u8, guard) else null;
        self.authority.request.session_id = self.session_id;
        self.native_adapter = .{ .server = server, .identity = &self.identity, .context = try self.authority.context(), .database = self.authority.request.database.?, .namespace = self.authority.request.namespace.?, .session_id = self.session_id };
        var transaction_lease: ?@import("transactions.zig").SessionRegistry.CommitExecution = null;
        defer if (transaction_lease) |lease| lease.release();
        if (self.session_id) |id_hex| {
            const id = @import("distributed_txn.zig").parseTxnIdHex(id_hex) catch return error.SqlTransactionNotActive;
            if (try server.txn_sessions.principalAccess(server.alloc, id, credential.sessionPrincipal()) != .allowed) return error.SqlTransactionNotActive;
            transaction_lease = server.txn_sessions.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
            var state = (try server.txn_sessions.getSqlState(server.alloc, id)) orelse return error.SqlTransactionNotActive;
            defer state.deinit(server.alloc);
            if (state.metadata.failed or state.terminal != null) return error.SqlTransactionAborted;
            if (!std.mem.eql(u8, state.metadata.database, request.database orelse "default") or !std.mem.eql(u8, state.metadata.namespace, request.session_namespace orelse request.namespace orelse "public")) return error.SqlTransactionNotActive;
            self.staged = try server.txn_sessions.cloneSqlStaged(alloc, id);
            self.native_adapter.active_transaction = id;
            self.native_adapter.staged = &self.staged;
            if (state.metadata.isolation != .read_committed) self.native_adapter.range_reads = &self.range_guards;
        }
        self.guarded = .{ .native = self.native_adapter.backend(), .authority = &self.authority, .revision = &self.native_adapter.revision, .expected_guard = self.authority.request.binding_guard };
        const parameters = try normalizeParameters(arena, request.parameters, request.parameter_types);
        const opened = try Pull.Stream.open(alloc, self.guarded.backend(), plan.compiled(), parameters, .{ .result_rows = request.limit, .page_rows = 256 });
        if (opened == null) {
            self.identity.?.deinit(server.alloc);
            self.arena.deinit();
            self.staged.deinit(alloc);
            self.range_guards.deinit(server.alloc);
            if (self.session_id) |id| server.alloc.free(id);
            alloc.destroy(self);
            return null;
        }
        self.stream = opened.?;
        errdefer self.stream.close();
        if (self.session_id) |id_hex| if (self.range_guards.tables.len != 0) {
            const id = try @import("distributed_txn.zig").parseTxnIdHex(id_hex);
            _ = (try server.txn_sessions.stage(server.alloc, id, &self.range_guards)) orelse return error.SqlTransactionNotActive;
        };
        if (transaction_lease) |lease| {
            lease.release();
            transaction_lease = null;
        }
        const binding = self.stream.context.binding;
        const policies = try arena.alloc(Policy, if (binding.relation) |relation| relation.scans.len else if (binding.table != null) 1 else 0);
        for (policies, 0..) |*policy, i| {
            const table = if (binding.relation) |relation| relation.scans[i].table else binding.table.?;
            policy.* = .{ .table = try arena.dupe(u8, table.physical_name), .filter = try http.resolveEffectiveRowFilterJson(arena, self.identity, table.physical_name) };
            if (table.scope) |scope| {
                policy.name = .{ .database = try arena.dupe(u8, scope.database), .namespace = try arena.dupe(u8, scope.namespace), .table = try arena.dupe(u8, scope.name) };
                policy.id = table.id;
                policy.schema_version = table.schema_version;
            }
        }
        self.policies = policies;
        const columns = try arena.alloc(wire.Column, self.stream.context.binding.columns.len);
        for (columns, self.stream.context.binding.columns) |*out, column| out.* = .{ .name = try arena.dupe(u8, column.name), .type = wireType(column.type) };
        self.columns = columns;
        self.plan = plan;
        self.admission = admission;
        moved = true;
        return .{ .context = self, .columns = columns, .next = next, .close = close, .detach = detach, .validate = validate };
    }

    fn validate(raw: *anyopaque, alloc: std.mem.Allocator, request: wire.Request) !void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        var job = StreamJob{ .adapter = self.adapter, .alloc = alloc, .credential = self.authority.credential, .request = request, .owner = self, .validate_only = true };
        try job.dispatch();
    }

    fn validateCursor(self: *OwnedRead, alloc: std.mem.Allocator, request: wire.Request) !void {
        try request.check();
        try self.authority.credential.validate();
        var admission: ?http.RequestAdmission.Lease = if (self.detached) try self.adapter.server.acquireSqlExecution(false) else null;
        defer if (admission) |*lease| lease.release();
        const original = self.authority.request;
        if (!std.mem.eql(u8, original.database.?, request.database orelse "default") or !std.mem.eql(u8, original.namespace.?, request.namespace orelse "public") or
            !std.mem.eql(u8, original.binding_guard orelse "", request.binding_guard orelse "")) return error.CatalogGenerationChanged;
        // Detached rows retain immutable source identity, not permission to
        // follow a renamed/replaced logical resource into a different table.
        // Resolve definitions only: never reacquire or rerun the row snapshot.
        var current: ?http.AuthenticatedIdentity = try self.authority.credential.identity(self.adapter.server.alloc);
        defer current.?.deinit(self.adapter.server.alloc);
        try validatePolicies(self.adapter.server.alloc, &current.?, self.identity.?, self.policies);
        var authority: Authority = .{ .credential = self.authority.credential, .identity = &current, .request = request };
        var adapter: execution.Adapter = .{ .server = self.adapter.server, .identity = &current, .context = try authority.context(), .database = original.database.?, .namespace = original.namespace.? };
        const catalog_backend = adapter.backend();
        for (self.policies) |policy| if (policy.name) |name| {
            const table = try catalog_backend.vtable.resolve(catalog_backend.ptr, alloc, name, .read);
            if (table.id != policy.id or table.schema_version != policy.schema_version or !std.mem.eql(u8, table.physical_name, policy.table)) return error.CatalogGenerationChanged;
        };
    }

    fn next(raw: *anyopaque, alloc: std.mem.Allocator, request: wire.Request, limit: u32) !wire.StreamPage {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        var job = StreamJob{ .adapter = self.adapter, .alloc = alloc, .credential = self.authority.credential, .request = request, .owner = self, .limit = limit };
        try job.dispatch();
        return job.page.?;
    }

    fn pull(self: *OwnedRead, alloc: std.mem.Allocator, request: wire.Request, limit: u32) !wire.StreamPage {
        if (self.detached) return error.InvalidSqlBackendResponse;
        try request.check();
        // Retained snapshots have a hard initial statement deadline, even if
        // later Execute messages carry a newer per-request deadline.
        try self.authority.request.check();
        try self.authority.credential.validate();
        if (self.session_id) |id_hex| {
            const id = try @import("distributed_txn.zig").parseTxnIdHex(id_hex);
            if (try self.adapter.server.txn_sessions.principalAccess(self.adapter.server.alloc, id, self.authority.credential.sessionPrincipal()) != .allowed) return error.SqlTransactionNotActive;
            var state = (try self.adapter.server.txn_sessions.getSqlState(self.adapter.server.alloc, id)) orelse return error.SqlTransactionNotActive;
            defer state.deinit(self.adapter.server.alloc);
            if (state.metadata.failed or state.terminal != null) return error.SqlTransactionNotActive;
        }
        var fresh = try self.authority.credential.identity(alloc);
        defer fresh.deinit(alloc);
        try validatePolicies(alloc, &fresh, self.identity.?, self.policies);
        var page = try self.stream.next(limit);
        errdefer page.deinit();
        // Adapt only datetime cells; the page retains exact typed integers.
        for (self.columns, 0..) |column, index| if (column.type == .datetime) {
            for (page.output.rows) |row| @constCast(row)[index] = try datetimeResult(page.arena.allocator(), row[index]);
        };
        const owner = try alloc.create(PageOwner);
        owner.* = .{ .alloc = alloc, .page = page };
        return .{ .exhausted = page.exhausted, .result = .{
            .columns = self.columns,
            .rows = page.output.rows,
            .sql_nulls = page.output.sql_nulls,
            .command_tag = "SELECT",
            .owner = .{ .context = owner, .release = releasePage },
        } };
    }

    fn validatePolicies(alloc: std.mem.Allocator, fresh: *http.AuthenticatedIdentity, admitted: http.AuthenticatedIdentity, policies: []const Policy) !void {
        // The catalog resolves logical resources to private physical names.
        // Reapply only the pinned aliases to fresh authority, never its old
        // permissions or filters. Buffered operator rows need this check too:
        // they need not cause another native cursor.next call.
        for (admitted.catalog_aliases) |alias| try http.projectCatalogIdentity(alloc, fresh, alias.logical, alias.physical);
        for (policies) |policy| {
            if (!(try http.tablePermissionCurrentlyAllowed(fresh.*, policy.table, .read))) return error.Forbidden;
            const filter = try http.resolveEffectiveRowFilterJson(alloc, fresh.*, policy.table);
            defer if (filter) |value| alloc.free(value);
            if ((filter == null) != (policy.filter == null) or
                (filter != null and !std.mem.eql(u8, filter.?, policy.filter.?))) return error.Forbidden;
        }
    }

    fn releasePage(raw: *anyopaque) void {
        const owner: *PageOwner = @ptrCast(@alignCast(raw));
        owner.page.deinit();
        owner.alloc.destroy(owner);
    }

    const PageOwner = struct { alloc: std.mem.Allocator, page: Pull.Page };

    fn detach(raw: *anyopaque) void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        if (self.detached) return;
        self.detached = true;
        self.stream.close();
        self.plan.release(self.adapter.server.sqlPlanCacheIo());
        self.admission.release();
        self.staged.deinit(self.alloc);
        self.range_guards.deinit(self.adapter.server.alloc);
    }

    fn close(raw: *anyopaque) void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        detach(raw);
        self.identity.?.deinit(self.adapter.server.alloc);
        if (self.session_id) |id| self.adapter.server.alloc.free(id);
        self.arena.deinit();
        self.alloc.destroy(self);
    }
};

const StreamJob = struct {
    adapter: *Adapter,
    alloc: std.mem.Allocator,
    credential: *Credential,
    request: wire.Request,
    owner: ?*OwnedRead = null,
    limit: u32 = 0,
    validate_only: bool = false,
    namespace_only: bool = false,
    opened: ?wire.ReadStream = null,
    page: ?wire.StreamPage = null,
    failure: ?anyerror = null,
    done: std.Io.Event = .unset,

    fn dispatch(self: *StreamJob) !void {
        try validateRequest(self.request);
        if (self.adapter.server.cfg.user_manager != self.credential.manager) return error.Unauthorized;
        const backend_runtime = self.adapter.server.cfg.backend_runtime orelse return error.SqlWriteCapacityUnavailable;
        const io = backend_runtime.io() orelse return error.SqlWriteCapacityUnavailable;
        var future = io.concurrent(run, .{self}) catch return error.SqlWriteCapacityUnavailable;
        self.done.waitUncancelable(self.request.io);
        _ = future.await(io);
        if (self.failure) |err| return err;
    }
    fn run(self: *StreamJob) void {
        defer self.done.set(self.request.io);
        self.runInner() catch |err| {
            self.failure = err;
            if (self.request.diagnostics) |diagnostic| {
                const safe = execution.diagnostic(err);
                var message: [256]u8 = undefined;
                diagnostic.set(safe.code, execution.diagnosticMessage(err, &message), null, safe.retryable);
            }
        };
    }
    fn runInner(self: *StreamJob) !void {
        if (self.namespace_only) {
            const server = self.adapter.server;
            const domain = @import("../system_catalog/domain.zig");
            const database = self.request.database orelse "default";
            const namespace = self.request.namespace orelse "public";
            try domain.validateName(database);
            try domain.validateName(namespace);
            var identity: ?http.AuthenticatedIdentity = try self.credential.identity(server.alloc);
            defer identity.?.deinit(server.alloc);
            const resource = try @import("../system_catalog/routes.zig").resourceNameAlloc(self.alloc, .{ .kind = .namespace, .database = database, .name = namespace });
            defer self.alloc.free(resource);
            if (!http.permissionsAllow(identity.?.permissions, .namespace, resource, .read)) return error.Forbidden;
            var admission = try server.acquireSqlExecution(false);
            defer admission.release();
            var authority: Authority = .{ .credential = self.credential, .identity = &identity, .request = self.request };
            const call = server.source.vtable.system_catalog orelse return error.UnsupportedSqlExecution;
            const response = try call(server.source.ptr, self.alloc, try authority.context(), .{ .read = .{ .kind = .namespace, .database = database, .name = namespace } });
            self.alloc.free(response);
            return;
        }
        if (self.validate_only) return self.owner.?.validateCursor(self.alloc, self.request);
        if (self.owner) |owner| self.page = try owner.pull(self.alloc, self.request, self.limit) else self.opened = try OwnedRead.open(self.adapter, self.alloc, self.credential, self.request);
    }
};

const Credential = struct {
    manager: *usermgr.UserManager,
    username: []u8,
    principal: []u8,
    verifier_mac: [Mac.mac_length]u8,

    fn sessionPrincipal(self: *const Credential) []const u8 {
        // Durable Basic SQL sessions use the username across mixed-version
        // rollouts. Credential-scoped caches and grants still use `principal`.
        return self.username;
    }

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
            .select, .explain => false,
            .insert, .update, .delete, .merge, .create_table, .drop_table, .catalog_ddl, .begin, .commit, .rollback, .savepoint, .rollback_to_savepoint, .release_savepoint, .set_constraints => true,
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
            .session_namespace = self.request.session_namespace,
        };
        var guarded = GuardedCatalog{
            .native = native_adapter.backend(),
            .authority = &authority,
            .revision = &native_adapter.revision,
            .expected_guard = self.request.binding_guard,
        };
        if (self.kind == .describe) {
            switch (compiled.statement) {
                .begin, .commit, .rollback, .savepoint, .rollback_to_savepoint, .release_savepoint, .set_constraints => {
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
                .binding_guard = try statementBindingGuard(self.alloc, native_adapter.revision, description.binding),
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

test "SQL pgwire execute arguments use bounded scalar semantics without table access" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var canceled: std.atomic.Value(bool) = .init(false);
    const request: wire.Request = .{ .statement = "SELECT $1, $2", .parameter_types = &.{ .integer, .string }, .limit = 1, .io = std.testing.io, .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } }, .cancel_requested = &canceled };
    const result = try Adapter.evaluateScalarParameters(arena.allocator(), request, &.{ "9007199254740991 + 2", "concat('a,b', upper('x'))" });
    try std.testing.expectEqual(@as(i64, 9007199254740993), result[0].integer);
    try std.testing.expectEqualStrings("a,bX", result[1].string);
    try std.testing.expectError(error.InvalidSqlParameters, Adapter.evaluateScalarParameters(arena.allocator(), request, &.{ "$1", "'x'" }));
    var json_request = request;
    json_request.parameter_types = &.{ .json, .json };
    const json_values = try Adapter.evaluateScalarParameters(arena.allocator(), json_request, &.{ "'null'::json", "'\"text\"'::json" });
    try std.testing.expectEqualStrings("null", json_values[0].string);
    try std.testing.expectEqualStrings("\"text\"", json_values[1].string);
    const null_values = try Adapter.evaluateScalarParameters(arena.allocator(), json_request, &.{ "NULL", "NULL::json" });
    try std.testing.expect(null_values[0] == .null and null_values[1] == .null);
    canceled.store(true, .release);
    try std.testing.expectError(error.QueryCanceled, Adapter.evaluateScalarParameters(arena.allocator(), request, &.{ "1", "'x'" }));
}

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
    inline for (.{ "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "SET CONSTRAINTS", "TRUNCATE TABLE" }) |tag| if (std.mem.eql(u8, command, tag)) return alloc.dupe(u8, command);
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
        var result = self.native;
        result.ptr = self;
        result.vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = openScan, .open_statement = openStatement, .mutate = mutate, .mutate_prepared = mutatePrepared, .prepare_mutations = prepareMutations, .resolve_conflict_owners = resolveConflictOwners, .generate_row_id = generateRowId, .checkpoint = checkpoint, .ddl = ddl };
        return result;
    }
    fn generateRowId(raw: *anyopaque, alloc: std.mem.Allocator) ![]const u8 {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        const generate = self.native.vtable.generate_row_id orelse return error.UnsupportedSqlExecution;
        return generate(self.native.ptr, alloc);
    }
    fn resolveConflictOwners(raw: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, columns: []const []const u8, expressions: []const catalog.ConflictExpression, arbiter_conditions: []const catalog.Condition, mutations: []const catalog.Mutation) ![]const catalog.ConflictOwner {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try self.checkRead(table.physical_name);
        if (!Authority.allowsWrite(self.authority, table.physical_name)) return error.Forbidden;
        const resolve_owners = self.native.vtable.resolve_conflict_owners orelse return error.UnsupportedSqlShape;
        return resolve_owners(self.native.ptr, alloc, table, columns, expressions, arbiter_conditions, mutations);
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
        // Materialized execution closes before returning; streaming execution
        // retains the heap-owned guard and authority in its OwnedRead capsule.
        return .{ .ptr = cursor, .next = ReadCursor.next, .close = ReadCursor.close };
    }
    fn mutate(raw: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        if (!Authority.allowsWrite(self.authority, table.physical_name)) return error.Forbidden;
        return self.native.vtable.mutate(self.native.ptr, alloc, table, input);
    }

    fn mutatePrepared(raw: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        if (!Authority.allowsWrite(self.authority, table.physical_name)) return error.Forbidden;
        const commit = self.native.vtable.mutate_prepared orelse return error.UnsupportedSqlExecution;
        return commit(self.native.ptr, alloc, table, input);
    }

    fn prepareMutations(raw: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) ![]const catalog.Mutation {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        if (!Authority.allowsWrite(self.authority, table.physical_name)) return error.Forbidden;
        const prepare = self.native.vtable.prepare_mutations orelse return error.UnsupportedSqlExecution;
        return prepare(self.native.ptr, alloc, table, input);
    }

    const StatementRead = struct {
        alloc: std.mem.Allocator,
        inner: catalog.StatementRead,
        wrappers: []ReadCursor,
        cursors: []catalog.Cursor,
        fn borrowedClose(_: *anyopaque) void {}
        fn close(raw: *anyopaque) void {
            const self: *StatementRead = @ptrCast(@alignCast(raw));
            self.inner.close(self.inner.ptr);
            for (self.wrappers) |wrapper| self.alloc.free(wrapper.table);
            self.alloc.free(self.wrappers);
            self.alloc.free(self.cursors);
            self.alloc.destroy(self);
        }
    };

    fn openStatement(raw: *anyopaque, alloc: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        for (scans) |scan_request| try self.checkRead(scan_request.table.physical_name);
        const open = self.native.vtable.open_statement orelse return error.SqlStatementSnapshotRequired;
        const inner = try open(self.native.ptr, alloc, scans);
        errdefer inner.close(inner.ptr);
        if (inner.cursors.len != scans.len) return error.InvalidSqlBackendResponse;
        const owner = try alloc.create(StatementRead);
        errdefer alloc.destroy(owner);
        const wrappers = try alloc.alloc(ReadCursor, scans.len);
        errdefer alloc.free(wrappers);
        const cursors = try alloc.alloc(catalog.Cursor, scans.len);
        errdefer alloc.free(cursors);
        var count: usize = 0;
        errdefer for (wrappers[0..count]) |wrapper| alloc.free(wrapper.table);
        for (scans, inner.cursors, wrappers, cursors) |scan_request, cursor, *wrapper, *out| {
            wrapper.* = .{ .alloc = alloc, .guard = self, .table = try alloc.dupe(u8, scan_request.table.physical_name), .inner = cursor };
            count += 1;
            out.* = .{ .ptr = wrapper, .next = ReadCursor.next, .close = StatementRead.borrowedClose };
        }
        owner.* = .{ .alloc = alloc, .inner = inner, .wrappers = wrappers, .cursors = cursors };
        return .{ .ptr = owner, .cursors = cursors, .close = StatementRead.close };
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

fn statementBindingGuard(alloc: std.mem.Allocator, revision: ?u64, binding: describe_sql.BoundStatement) !?[]const u8 {
    var scans: [64]catalog.StatementScan = undefined;
    var count: usize = 0;
    try collectBindingTables(binding, &scans, &count, 0);
    if (count == 0) return null;
    return try bindingGuards(alloc, revision orelse return error.InvalidSqlBackendResponse, scans[0..count]);
}

fn collectBindingTable(table: catalog.Table, scans: *[64]catalog.StatementScan, count: *usize) !void {
    for (scans[0..count.*]) |scan| {
        if (scan.table.id == table.id and scan.table.schema_version == table.schema_version and std.mem.eql(u8, scan.table.physical_name, table.physical_name)) return;
    }
    if (count.* == scans.len) return error.SqlProgramLimitExceeded;
    scans[count.*] = .{ .table = table, .request = .{ .fields = &.{}, .limit = 1 } };
    count.* += 1;
}

fn collectBindingTables(binding: describe_sql.BoundStatement, scans: *[64]catalog.StatementScan, count: *usize, depth: usize) anyerror!void {
    if (depth >= 64) return error.SqlProgramLimitExceeded;
    if (binding.table) |table| try collectBindingTable(table, scans, count);
    if (binding.relation) |relation| for (relation.scans) |scan| {
        try collectBindingTable(scan.table, scans, count);
    };
    if (binding.insert_source) |source| try collectBindingTables(source.*, scans, count, depth + 1);
    if (binding.joined_mutation) |joined| try collectBindingTables(joined.input.*, scans, count, depth + 1);
    if (binding.returning) |returning| try collectBindingTables(returning.*, scans, count, depth + 1);
}

fn bindingGuard(alloc: std.mem.Allocator, revision: u64, table: catalog.Table) ![]const u8 {
    return bindingGuards(alloc, revision, &.{.{ .table = table, .request = .{ .fields = &.{}, .limit = 1 } }});
}

fn bindingGuards(alloc: std.mem.Allocator, revision: u64, scans: []const catalog.StatementScan) ![]const u8 {
    if (scans.len == 0 or scans.len > 64) return error.SqlProgramLimitExceeded;
    var length: usize = 10;
    for (scans) |scan| length = try std.math.add(usize, length, 16 + scan.table.physical_name.len);
    const bytes = try alloc.alloc(u8, length);
    std.mem.writeInt(u64, bytes[0..8], revision, .little);
    std.mem.writeInt(u16, bytes[8..10], @intCast(scans.len), .little);
    var offset: usize = 10;
    for (scans) |scan| {
        const entry = bytes[offset..];
        std.mem.writeInt(u32, entry[0..4], @intCast(scan.table.physical_name.len), .little);
        std.mem.writeInt(u64, entry[4..12], scan.table.id, .little);
        std.mem.writeInt(u32, entry[12..16], scan.table.schema_version, .little);
        @memcpy(entry[16..][0..scan.table.physical_name.len], scan.table.physical_name);
        offset += 16 + scan.table.physical_name.len;
    }
    return bytes;
}

fn verifyBindingGuard(guard: []const u8, revision: u64, table: catalog.Table) !void {
    if (guard.len < 10 or std.mem.readInt(u64, guard[0..8], .little) != revision) return error.CatalogGenerationChanged;
    const count = std.mem.readInt(u16, guard[8..10], .little);
    if (count == 0 or count > 64) return error.CatalogGenerationChanged;
    var rest = guard[10..];
    var found = false;
    for (0..count) |_| {
        if (rest.len < 16) return error.CatalogGenerationChanged;
        const length = std.mem.readInt(u32, rest[0..4], .little);
        if (length > rest.len - 16) return error.CatalogGenerationChanged;
        if (std.mem.eql(u8, rest[16..][0..length], table.physical_name)) {
            if (std.mem.readInt(u64, rest[4..12], .little) != table.id or
                std.mem.readInt(u32, rest[12..16], .little) != table.schema_version) return error.CatalogGenerationChanged;
            found = true;
        }
        rest = rest[16 + length ..];
    }
    if (rest.len != 0 or !found) return error.CatalogGenerationChanged;
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
    const other: catalog.Table = .{ .id = 55, .physical_name = "other_docs", .schema_version = 3, .columns = &.{} };
    const many = try bindingGuards(std.testing.allocator, 7, &.{
        .{ .table = table, .request = .{ .fields = &.{}, .limit = 1 } },
        .{ .table = other, .request = .{ .fields = &.{}, .limit = 1 } },
    });
    defer std.testing.allocator.free(many);
    try verifyBindingGuard(many, 7, other);
    try verifyBindingGuard(many, 7, table);
    for (0..many.len) |length| try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard(many[0..length], 7, other));
}

test "SQL pgwire binding manifest includes recursive mutation sources and deduplicates targets" {
    const target: catalog.Table = .{ .id = 1, .physical_name = "target", .schema_version = 1, .columns = &.{} };
    const source: catalog.Table = .{ .id = 2, .physical_name = "source", .schema_version = 1, .columns = &.{} };
    const source_binding: describe_sql.BoundStatement = .{ .table = source, .action = .read, .columns = &.{}, .parameter_types = &.{}, .json_literals = .empty };
    var binding: describe_sql.BoundStatement = .{ .table = target, .action = .read, .columns = &.{}, .parameter_types = &.{}, .json_literals = .empty, .insert_source = &source_binding, .returning = &source_binding };
    const inserted = (try statementBindingGuard(std.testing.allocator, 9, binding)).?;
    defer std.testing.allocator.free(inserted);
    try std.testing.expectEqual(@as(u16, 2), std.mem.readInt(u16, inserted[8..10], .little));
    try verifyBindingGuard(inserted, 9, target);
    try verifyBindingGuard(inserted, 9, source);
    var replaced = source;
    replaced.id = 3;
    try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard(inserted, 9, replaced));
    var joined: @import("../sql/joined_mutation.zig").Bound = undefined;
    joined.input = &source_binding;
    binding.insert_source = null;
    binding.returning = null;
    binding.joined_mutation = &joined;
    const updated = (try statementBindingGuard(std.testing.allocator, 9, binding)).?;
    defer std.testing.allocator.free(updated);
    try std.testing.expectEqualSlices(u8, inserted, updated);
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
    var projected = (try http.cloneCatalogIdentity(alloc, admitted)).?;
    defer projected.deinit(alloc);
    try http.projectCatalogIdentity(alloc, &projected, "docs", "private-table-17");
    const retained_policies = [_]OwnedRead.Policy{.{ .table = "private-table-17", .filter = null }};
    {
        var current = try credential.identity(alloc);
        defer current.deinit(alloc);
        try OwnedRead.validatePolicies(alloc, &current, projected, &retained_policies);
    }
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
    {
        var current = try credential.identity(alloc);
        defer current.deinit(alloc);
        try std.testing.expectError(error.Forbidden, OwnedRead.validatePolicies(alloc, &current, projected, &retained_policies));
    }
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
        .{ .command = "SET CONSTRAINTS", .expected = "SET CONSTRAINTS" },
        .{ .command = "TRUNCATE TABLE", .expected = "TRUNCATE TABLE" },
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
