// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const storage_schema = @import("antfly_local_sources").storage_schema;
const http = @import("http_server.zig");
const execution = @import("sql_execution.zig");
const compiler = @import("antfly_local_sources").sql_compiler;
const describe_sql = @import("antfly_local_sources").sql_describe;
const native = @import("antfly_local_sources").sql_runtime;
const catalog = @import("antfly_local_sources").sql_catalog;
const ast = @import("antfly_local_sources").sql_ast;
const operation = @import("antfly_local_sources").api_operation;
const usermgr = @import("../usermgr/mod.zig");
const io_abi = @import("antfly_runtime_abi").io_abi;
const Mac = std.crypto.auth.hmac.sha2.HmacSha256;
const credential_domain = "antfly.pgwire.password-session.v1";

test {
    _ = @import("../pgwire/protocol.zig");
    _ = @import("../pgwire/values.zig");
}

test "SQL NUMERIC public result descriptors parse through generated OpenAPI contracts" {
    const a = std.testing.allocator;
    for ([_]ast.ColumnType{ .number, .array }) |kind| {
        const encoded = try std.json.Stringify.valueAlloc(a, describe_sql.Column{ .name = "n", .type = kind, .element_type = .numeric, .numeric_modifier = .{ .precision = 2, .scale = -3 } }, .{});
        defer a.free(encoded);
        const parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").SQLColumn, a, encoded, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(i64, 2), parsed.value.numeric_modifier.?.precision);
        try std.testing.expectEqual(@as(i64, -3), parsed.value.numeric_modifier.?.scale);
    }
}

/// Stable API-owned callback and listener storage. Shutdown joins all native
/// work before releasing the adapter, user manager, or backend runtime.
pub const Listener = struct {
    alloc: std.mem.Allocator,
    adapter: Adapter,
    server: @import("../pgwire/server.zig").Server,
    transport_io: std.Io.Threaded,

    pub fn start(api: *http.ApiHttpServer, config: @import("antfly_local_sources").common_config.Config.PgwireConfig) !*Listener {
        if (api.cfg.user_manager == null) return error.PgwireRequiresAuthentication;
        if (api.cfg.backend_runtime == null) return error.PgwireRequiresBackendRuntime;
        _ = api.sharedApiNetworkIo() orelse return error.PgwireRequiresBackendRuntime;
        // Raw protocol/frame/runtime allocations must remain possible after
        // foreground node memory is full, before a control can be classified.
        // Prepared execution state still follows the adapter's API owners.
        const transport_alloc = if (api.transport_memory) |memory| memory.allocator() else api.state_alloc;
        const self = try transport_alloc.create(Listener);
        errdefer transport_alloc.destroy(self);
        self.alloc = transport_alloc;
        self.adapter = .{ .server = api };
        // Long-lived idle sessions must not consume the shared outbound lane.
        // One session task per connection plus accept, deadline and cancel slots.
        self.transport_io = @import("../pgwire/server.zig").initTransport(self.alloc, config.max_connections);
        errdefer self.transport_io.deinit();
        const io = self.transport_io.io();
        self.server = try @import("../pgwire/server.zig").start(self.alloc, .{
            .io = io,
            .backend = self.adapter.backend(),
            .bind_host = config.bind_host orelse "127.0.0.1",
            .bind_port = config.bind_port,
            .max_connections = config.max_connections,
            .max_session_bytes = config.max_session_bytes,
            .max_retained_bytes = config.max_retained_bytes,
            .protected_frame_allocator = if (api.parser_memory) |memory| memory.allocator() else null,
            .control_allocator = api.controlAllocator(),
            .is_protected_control = @import("sql_control.zig").classify,
            .limits = .{ .retained_bytes = config.retained_bytes_per_connection, .idle_transaction_timeout_ms = config.idle_transaction_timeout_ms },
            .allow_insecure_non_loopback = config.externally_protected_transport,
        });
        return self;
    }

    pub fn deinit(self: *Listener) void {
        self.server.deinit();
        self.transport_io.deinit();
        self.alloc.destroy(self);
    }
};

fn resolveOnPath(native_backend: catalog.Backend, alloc: std.mem.Allocator, name: ast.Name, action: catalog.Action, path: ?*const @import("../pgwire/search_path.zig").Path) !catalog.Table {
    if (name.namespace != null or name.database != null or path == null) return native_backend.vtable.resolve(native_backend.ptr, alloc, name, action);
    // All candidates use the same native catalog-revision fence. A missing
    // relation may advance; auth, schema churn, and runtime failures may not.
    const selected = path.?;
    for (selected.entries[0..selected.len]) |*entry| {
        var scoped = name;
        scoped.namespace = entry.slice();
        const table = native_backend.vtable.resolve(native_backend.ptr, alloc, scoped, action) catch |err| switch (err) {
            error.TableNotFound => continue,
            else => return err,
        };
        return table;
    }
    return error.TableNotFound;
}

test "SQL pgwire ordered native resolver falls back only on exact table absence" {
    // sql-0038 / sql-0040: SET SESSION and SET LOCAL share this ordered,
    // authorization-fenced native binding path after pgwire scope selection.
    const search_path = @import("../pgwire/search_path.zig");
    const Fixture = struct {
        calls: usize = 0,
        seen: [4]search_path.Namespace = undefined,
        first_error: ?anyerror = null,

        fn resolve(raw: *anyopaque, _: std.mem.Allocator, name: ast.Name, _: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.seen[self.calls] = try search_path.Namespace.init(name.namespace orelse "public");
            self.calls += 1;
            if (self.calls == 1) if (self.first_error) |err| return err;
            return .{ .id = 7, .physical_name = "second_table", .schema_version = 1, .columns = &.{} };
        }
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = undefined, .mutate = undefined, .checkpoint = undefined } };
        }
    };
    var path: search_path.Path = .{};
    try path.append(try search_path.Namespace.init("tenant_schema"));
    try path.append(try search_path.Namespace.init("public"));
    var fixture: Fixture = .{ .first_error = error.TableNotFound };
    const table = try resolveOnPath(fixture.backend(), std.testing.allocator, .{ .table = "t" }, .read, &path);
    try std.testing.expectEqual(@as(u64, 7), table.id);
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
    try std.testing.expectEqualStrings("tenant_schema", fixture.seen[0].slice());
    try std.testing.expectEqualStrings("public", fixture.seen[1].slice());

    for ([_]anyerror{ error.Forbidden, error.CatalogGenerationChanged, error.QueryCanceled }) |failure| {
        fixture = .{ .first_error = failure };
        try std.testing.expectError(failure, resolveOnPath(fixture.backend(), std.testing.allocator, .{ .table = "t" }, .read, &path));
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    }
    fixture = .{ .first_error = error.TableNotFound };
    try std.testing.expectError(error.TableNotFound, resolveOnPath(fixture.backend(), std.testing.allocator, .{ .namespace = "explicit", .table = "t" }, .read, &path));
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    try std.testing.expectEqualStrings("explicit", fixture.seen[0].slice());
}

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
        return .{ .context = self, .vtable = &.{ .native_parameters = true, .deliver_row = deliverRow, .authenticate = authenticate, .describe = describe, .execute = execute, .load_settings = loadSettings, .validate_namespace = validateNamespace, .evaluate_parameters = evaluateParameters, .evaluate_parameter_cells = evaluateParameterCells, .fail_transaction = failTransaction, .open_stream = openStream, .disconnect = disconnect } };
    }

    fn deliverRow(raw: *anyopaque, identity: wire.Identity, request: wire.Request, work: wire.DeliveryWork) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        try validateRequest(request);
        if (self.server.cfg.user_manager != credential.manager) return error.Unauthorized;
        var job: DeliveryJob = .{ .adapter = self, .credential = credential, .request = request, .work = work };
        const protected = deliveryIsControl(request);
        const io = if (protected) try self.server.sqlControlIo() else try self.server.sqlExecutionIo();
        var future = io.concurrent(DeliveryJob.run, .{&job}) catch return error.SqlWriteCapacityUnavailable;
        job.done.waitUncancelable(request.io);
        @import("protected_future.zig").wait(io, &future);
        if (job.failure) |err| return wireFailure(request, err);
    }

    fn loadSettings(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !@import("antfly_local_sources").sql_setting_catalog.RawSnapshot {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        var authenticated: ?http.AuthenticatedIdentity = try credential.identity(self.server.alloc);
        defer authenticated.?.deinit(self.server.alloc);
        var authority: Authority = .{ .credential = credential, .identity = &authenticated, .request = request };
        var context = try authority.context();
        context.setting_read_principal = credential.sessionPrincipal();
        const scope: @import("antfly_local_sources").sql_setting_catalog.Scope = .{ .principal = credential.sessionPrincipal(), .database = request.database orelse "default" };
        const bytes = try self.server.source.systemCatalog(alloc, context, .{ .setting_snapshot = scope });
        defer alloc.free(bytes);
        return std.json.parseFromSliceLeaky(@import("antfly_local_sources").sql_setting_catalog.RawSnapshot, alloc, bytes, .{ .allocate = .alloc_always });
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

    fn evaluateParameterCells(_: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request, expressions: []const []const u8) ![]const @import("antfly_local_sources").sql_scalar.Datum {
        const credential: *Credential = @ptrCast(@alignCast(identity.context));
        try credential.validate();
        return evaluateScalarCells(alloc, request, expressions);
    }

    fn evaluateScalarCells(alloc: std.mem.Allocator, request: wire.Request, expressions: []const []const u8) ![]const @import("antfly_local_sources").sql_scalar.Datum {
        if (expressions.len != request.parameter_types.len) return error.InvalidSqlParameters;
        const scalar = @import("antfly_local_sources").sql_scalar;
        const result = try alloc.alloc(scalar.Datum, expressions.len);
        if (request.parameter_descriptors.len != 0 and request.parameter_descriptors.len != expressions.len) return error.InvalidSqlParameters;
        for (expressions, request.parameter_types, result, 0..) |expression, kind, *value, index| {
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
            const descriptor: ?scalar.Type = if (request.parameter_descriptors.len != 0) request.parameter_descriptors[index] else if (expected) |type_| .{ .kind = type_ } else null;
            var program = try scalar.bindTypedExpectedWithSettings(alloc, compiled.expression, &.{}, &.{}, descriptor, .{}, null);
            defer program.deinit();
            const evaluated = try program.evaluate(alloc, &.{}, &.{}, .{});
            var retained = evaluated;
            retained.patterns = null;
            value.* = try @import("antfly_local_sources").sql_operators.cloneDatum(alloc, retained);
        }
        try request.check();
        return result;
    }

    fn evaluateScalarParameters(alloc: std.mem.Allocator, request: wire.Request, expressions: []const []const u8) ![]const std.json.Value {
        const cells = try evaluateScalarCells(alloc, request, expressions);
        const result = try alloc.alloc(std.json.Value, cells.len);
        for (cells, request.parameter_types, result) |evaluated, kind, *value| {
            value.* = if (evaluated.array) |array|
                try @import("antfly_local_sources").sql_array_wire.toJsonLeaky(alloc, array.*, .{})
            else if (evaluated.numeric) |number| numeric: {
                const exact = @import("antfly_local_sources").sql_numeric_value;
                var context: exact.Context = .{ .alloc = alloc };
                break :numeric .{ .number_string = try exact.format(&context, number.*) };
            } else if (kind == .json and !evaluated.sql_null)
                .{ .string = try std.json.Stringify.valueAlloc(alloc, evaluated.value, .{}) }
            else
                try native.clone(alloc, evaluated.value);
        }
        try request.check();
        return result;
    }

    fn openStream(raw: *anyopaque, alloc: std.mem.Allocator, identity: wire.Identity, request: wire.Request) !?wire.ReadStream {
        // Protocol probes streams before execute, including for COMMIT. A
        // control must reach its protected execution path without fresh slots.
        if (@import("sql_control.zig").classify(request.statement)) return null;
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
        var io = self.server.sqlControlIo() catch {
            // Cleanup is local owner release, never durable mutation replay.
            // Scheduling exhaustion must not silently abandon an idle session.
            const access = self.server.txn_sessions.principalAccess(self.server.alloc, id, credential.sessionPrincipal()) catch return;
            if (access == .allowed) _ = self.server.txn_sessions.removeBeforeExecution(self.server.alloc, id);
            return;
        };
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
        var future = io.concurrent(Cleanup.run, .{&job}) catch {
            Cleanup.run(&job);
            return;
        };
        job.done.waitUncancelable(network_io);
        @import("protected_future.zig").wait(io, &future);
    }

    fn dispatch(self: *Adapter, job: *Job) !void {
        try validateRequest(job.request);
        if (self.server.cfg.user_manager != job.credential.manager) return error.Unauthorized;
        const protected = @import("sql_control.zig").classify(job.request.statement);
        var io = if (protected) try self.server.sqlControlIo() else try self.server.sqlExecutionIo();
        var future = io.concurrent(Job.run, .{job}) catch return error.SqlWriteCapacityUnavailable;
        // Keep borrowed request storage alive through a durable decision. The
        // listener sets cancellation/deadline on shutdown, native checkpoints
        // observe it, and this adapter joins rather than abandoning the job.
        job.done.waitUncancelable(job.request.io);
        @import("protected_future.zig").wait(io, &future);
        if (job.failure) |err| return wireFailure(job.request, err);
    }
};

// Worker/admission/callback cancellation can be independent of the listener's
// flag. Preserve the frontend I/O authority before choosing protocol shutdown.
fn wireFailure(request: wire.Request, failure: anyerror) anyerror {
    return @import("../pgwire/operation_error.zig").fromBackend(request.io, failure);
}

const Pull = @import("antfly_local_sources").sql_read_stream;

fn cloneSettingOverlay(alloc: std.mem.Allocator, entries: []const @import("antfly_local_sources").sql_setting_catalog.OverlayEntry, owner: *@import("sql_preparation.zig").Owner) ![]const @import("antfly_local_sources").sql_setting_catalog.OverlayEntry {
    try owner.check();
    const result = try alloc.alloc(@import("antfly_local_sources").sql_setting_catalog.OverlayEntry, entries.len);
    for (entries, result) |entry, *out| {
        try owner.check();
        out.* = entry;
        if (entry.value == .string) out.value = .{ .string = try owner.copy(alloc, entry.value.string) };
    }
    try owner.check();
    return result;
}

fn requestDeadlineNs(request: wire.Request) ?u64 {
    if (request.deadline.raw.nanoseconds == std.math.maxInt(i96)) return null;
    // A finite timestamp outside the native clock range remains a finite
    // ceiling. It must neither trap nor become an absent deadline.
    return std.math.cast(u64, @max(@as(i96, 0), request.deadline.raw.nanoseconds)) orelse std.math.maxInt(u64);
}

fn admissionOptions(request: wire.Request) @import("antfly_local_sources").common_bounded_admission.Options {
    return admissionOptionsWithBytes(request, @as(u64, request.statement.len) +| @import("antfly_local_sources").common_bounded_admission.parameterBytes(request.parameters) +| 1024);
}

/// Cursor page commands carry new restrictions on the retained request, while
/// its immutable input charge belongs to the original cursor capsule.
fn admissionOptionsWithBytes(request: wire.Request, retained_input_bytes: u64) @import("antfly_local_sources").common_bounded_admission.Options {
    return .{
        .io = request.io,
        .deadline_ns = requestDeadlineNs(request),
        .cancellation = .{ .ptr = request.cancel_requested, .requested = struct {
            fn call(raw: *const anyopaque) bool {
                const signal: *const std.atomic.Value(bool) = @ptrCast(@alignCast(raw));
                return signal.load(.acquire);
            }
        }.call },
        .retained_bytes = retained_input_bytes,
    };
}

test "pgwire native array page delivery preserves dimensions NULLs and exact integers" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const alloc = arena.allocator();
            const sources = @import("antfly_local_sources");
            const array = (try sources.sql_array_text.decodeLeaky(alloc, .int64, "[-1:1]={9007199254740993,NULL,7}", .{})).value;
            const values = [_]sources.sql_scalar.Datum{.{ .array = &array, .sql_null = false }};
            var page: Pull.BatchPage = .{
                .arena = std.heap.ArenaAllocator.init(a),
                .columns = &.{.{ .name = "a", .type = .array, .element_type = .int64 }},
                .values = .{ .rows = &.{&values} },
                .exhausted = true,
            };
            defer page.deinit();
            const cell = try OwnedRead.PageOwner.pageCell(&page, alloc, 0, 0);
            try std.testing.expect(!cell.sql_null);
            try std.testing.expectEqual(std.json.Value.null, cell.value);
            try std.testing.expect(cell.array != null);
            var work: sources.sql_array_value.Budget = .{};
            try std.testing.expectEqual(std.math.Order.eq, try array.compare(cell.array.?.*, &work));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

/// The portal owns this entire capsule. No cursor retains Job.runInner's stack
/// identity, authority, schema binding or temporary request context.
const OwnedRead = struct {
    read_memory: ?@import("../common/read_memory.zig").Group = null,
    retained_lease: wire.RetainedLifetime.Lease,
    alloc: std.mem.Allocator,
    adapter: *Adapter,
    arena: std.heap.ArenaAllocator,
    identity: ?http.AuthenticatedIdentity,
    authority: Authority,
    native_adapter: execution.Adapter,
    runnable: http.RunnableScheduler.Execution,
    retained_input_bytes: u64,
    decision_runtime: ?@import("antfly_local_sources").functions_runtime.Runtime = null,
    session_id: ?[]u8 = null,
    staged: @import("transactions.zig").OwnedTransactionCommitRequest = .{},
    range_guards: @import("transactions.zig").OwnedTransactionCommitRequest = .{},
    guarded: GuardedCatalog,
    plan: @import("../sql/plan_cache.zig").Lease,
    admission: http.RequestAdmission.Lease,
    stream: *Pull.Stream,
    stream_live: bool = false,
    columns: []const wire.Column,
    policies: []const Policy,
    lifetime: @import("runnable_read_lifetime.zig").Lifetime,
    cleanup: ?@import("runnable_cleanup_owner.zig").Registry.Reservation = null,
    cleanup_started: bool = false,
    physical_retired: bool = false,
    retirement_mutex: std.atomic.Mutex = .unlocked,
    remote_client: ?@import("remote_attempt_client.zig").Client = null,
    remote_owner: ?*@import("remote_read_operation_owner.zig").Owner = null,
    root_retirement_ready: bool = false,
    root_retired: bool = false,
    retirement_io: std.Io,
    remote_dispatch: ?@import("retained_query_dispatch.zig").Dispatcher = null,
    root_retirement_done: std.Io.Event = .unset,
    const Policy = struct { table: []const u8, filter: ?[]const u8, name: ?ast.Name = null, id: u64 = 0, schema_version: u32 = 0 };

    fn open(adapter: *Adapter, alloc: std.mem.Allocator, credential: *Credential, request: wire.Request) !?wire.ReadStream {
        const server = adapter.server;
        var preparation = server.sql_preparation_admission.tryAcquireLease() orelse return error.SqlWriteCapacityUnavailable;
        defer preparation.release();
        var moved = false;
        var retained_lease = if (request.retained_lifetime) |lifetime| lifetime.retain() else wire.RetainedLifetime.Lease{};
        defer retained_lease.release();
        const self = try alloc.create(OwnedRead);
        defer if (!moved) alloc.destroy(self);
        self.alloc = alloc;
        self.runnable = .{};
        defer if (!moved) self.runnable.finish();
        // Start at the cursor's final address. The original root and its
        // request authority remain stable through preparation and delivery.
        self.identity = null;
        self.authority = .{ .credential = credential, .identity = &self.identity, .request = request };
        const preparation_options = admissionOptions(self.authority.request);
        self.retained_input_bytes = preparation_options.retained_bytes;
        try server.startSqlRunnable(&self.runnable, preparation_options, .query);
        var preparation_owner: @import("sql_preparation.zig").Owner = .{ .execution = &self.runnable, .options = preparation_options };
        var diagnostic: compiler.Diagnostic = .{};
        var plan = server.sqlPlanCache().acquireControlled(server.sqlPlanCacheIo(), .{ .statement = request.statement, .principal = credential.principal, .database = request.database orelse "default", .namespace = request.namespace orelse "public" }, &diagnostic, preparation_owner.control()) catch |err| return preparation_owner.original(err);
        defer if (!moved) plan.release(server.sqlPlanCacheIo());
        if (plan.compiled().statement != .select) return null;
        preparation.release();
        try @import("sql_read_lane.zig").apply(&self.runnable, plan.compiled());
        var admission = try server.acquireSqlExecutionUnderRunnable(&self.runnable, false, false, preparation_options);
        defer if (!moved) admission.release();
        var setup_owner: @import("sql_preparation.zig").Owner = .{ .execution = &self.runnable, .options = preparation_options };
        // A retained cursor must not borrow connection-owned credential memory.
        // Its copy uses the API allocator and still observes live revocation.
        const owned_credential = try credential.cloneOwned(server.alloc);
        errdefer if (!moved) Credential.release(owned_credential, server.alloc);
        self.authority.credential = owned_credential;
        self.session_id = null;
        self.staged = .{};
        self.range_guards = .{};
        self.lifetime = .{ .context = self, .retire = retirePhysical, .destroy = destroy };
        self.cleanup = null;
        self.cleanup_started = false;
        self.physical_retired = false;
        self.initRetirement(request.io);
        self.adapter = adapter;
        self.read_memory = null;
        if (server.readMemoryPool()) |pool| {
            self.read_memory = try @import("../common/read_memory.zig").Group.init(pool, .general, pool.limits.bytes[0]);
        }
        errdefer if (!moved) {
            if (self.read_memory) |*group| group.deinit();
        };
        self.arena = std.heap.ArenaAllocator.init(self.readAllocator());
        errdefer if (!moved) self.arena.deinit();
        errdefer if (!moved) self.staged.deinit(alloc);
        errdefer if (!moved) self.range_guards.deinit(server.alloc);
        const arena = self.arena.allocator();
        if (request.session_id) |encoded| self.session_id = try setup_owner.copy(server.alloc, encoded);
        errdefer if (!moved) {
            if (self.session_id) |id| server.alloc.free(id);
        };
        // Native catalog resolution extends this identity with aliases using
        // server.alloc; every allocation in the mutable identity must share it.
        self.identity = try owned_credential.identity(server.alloc);
        errdefer if (!moved) self.identity.?.deinit(server.alloc);
        // Statement, parameter and binding-guard data are portal-owned. Keep a
        // private request capsule so I/O borrows point to a stable address.
        self.authority.request.statement = try setup_owner.copy(arena, request.statement);
        self.authority.request.database = try setup_owner.copy(arena, request.database orelse "default");
        self.authority.request.namespace = try setup_owner.copy(arena, request.namespace orelse "public");
        self.authority.request.session_namespace = if (request.session_namespace) |scope| try setup_owner.copy(arena, scope) else null;
        self.authority.request.binding_guard = if (request.binding_guard) |guard| try setup_owner.copy(arena, guard) else null;
        self.authority.request.setting_overlay = try cloneSettingOverlay(arena, request.setting_overlay, &setup_owner);
        self.authority.request.session_id = self.session_id;
        try request.read_requirements.validate(.sql);
        self.native_adapter = .{ .runnable = if (self.runnable.entry != null) &self.runnable else null, .server = server, .identity = &self.identity, .context = try self.authority.context(), .database = self.authority.request.database.?, .namespace = self.authority.request.namespace.?, .session_id = self.session_id, .read_requirements = request.read_requirements, .shared_receipt_wait_budget = request.read_receipt_budget, .lake_visibility = if (request.read_requirements.visibility) |visibility| if (visibility == .accepted) .accepted else .committed else @fromBackingInt(@intCast(@backingInt(request.lake_visibility))), .setting_overlay = self.authority.request.setting_overlay, .setting_overlay_source = .connection, .expected_setting_epoch = self.authority.request.setting_epoch };
        if (self.native_adapter.runnable != null) {
            // Async closure borrows the connection's allocator and listener
            // adapter. A production session supplies the lifetime fence that
            // holds both through physical cleanup and joined task metadata.
            if (request.retained_lifetime == null) return error.SqlWriteCapacityUnavailable;
            const owner = try server.sqlCleanupOwner();
            const cleanup_io = try server.sqlCleanupIo();
            const cleanup_borrow = @import("antfly_runtime_abi").native_abi.IoBorrow.init(&cleanup_io);
            const borrow = self.native_adapter.providerRunnable() orelse return error.ExecutionWaitUnsupported;
            self.cleanup = try owner.registry.reserve(cleanup_io, borrow, &.{ .io = &cleanup_borrow, .activation_budget = 1, .deadline_ns = 30 * std.time.ns_per_s }, self, cleanupCallbacks);
        }
        // Everything needed for retirement is now initialized at its final
        // address. Publish the pre-reserved supervisor BEFORE any capture can
        // publish remote work, even if no stream is ultimately returned.
        self.publishSetup(plan, admission, &retained_lease);
        moved = true;
        var setup_owned = true;
        errdefer if (setup_owned) self.retireSetup();
        // Freeze protocol activation at setup, but acquire no remote metadata
        // for a local-only query. The foreground already pins server lifetime.
        self.remote_client = server.remoteRetainedReadClient();
        if (self.remote_client != null)
            self.native_adapter.read_remote_setup = .{ .ptr = self, .prepare = prepareRemoteReads };
        if (self.native_adapter.runnable != null) try self.native_adapter.beginReadFence();
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
            if (state.metadata.accepted_lake_reads) {
                if (request.read_requirements.visibility == .committed) return error.ReadSnapshotTooOld;
                self.native_adapter.lake_visibility = .accepted;
            }
            self.native_adapter.accepted_lake_repeatable = self.native_adapter.lake_visibility == .accepted and (state.metadata.isolation != .serializable or state.metadata.mode == .read_only);
            self.native_adapter.active_transaction = id;
            self.native_adapter.staged = &self.staged;
            if (state.metadata.isolation != .read_committed) self.native_adapter.range_reads = &self.range_guards;
        }
        self.decision_runtime = try self.native_adapter.decisionRuntime();
        if (self.decision_runtime) |*active| self.native_adapter.decision_provider = active.provider();
        self.guarded = .{ .native = self.native_adapter.backend(), .authority = &self.authority, .revision = &self.native_adapter.revision, .expected_guard = self.authority.request.binding_guard, .cleanup = &self.runnable };
        const parameters = if (request.parameter_cells != null) request.parameters else try normalizeParametersControlled(arena, request.parameters, request.parameter_types, &setup_owner);
        var stream_backend = self.guarded.backend();
        stream_backend.parameter_descriptor_hints = request.parameter_descriptors;
        if (plan.compiled().uses_current_setting) stream_backend.setting_capture = self.native_adapter.settingCapture();
        // Stream.open captures the owner view once; loadSettings validates the
        // prepared epoch on that same snapshot before binding or reading rows.
        if (!try self.captureStream(stream_backend, plan.compiled(), parameters, .{ .result_rows = request.limit, .page_rows = 256, .parameter_cells = request.parameter_cells, .parameter_owner = request.parameter_owner })) {
            // Returning materialized fallback is also a retained retirement
            // path: a failed/unsupported open can already own remote work.
            if (transaction_lease) |lease| {
                lease.release();
                transaction_lease = null;
            }
            try self.retireForFallback(&setup_owned, request);
            return null;
        }
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
            try setup_owner.check();
            const table = if (binding.relation) |relation| relation.scans[i].table else binding.table.?;
            policy.* = .{ .table = try setup_owner.copy(arena, table.physical_name), .filter = try http.resolveEffectiveRowFilterJson(arena, self.identity, table.physical_name) };
            if (table.scope) |scope| {
                policy.name = .{ .database = try setup_owner.copy(arena, scope.database), .namespace = try setup_owner.copy(arena, scope.namespace), .table = try setup_owner.copy(arena, scope.name) };
                policy.id = table.id;
                policy.schema_version = table.schema_version;
            }
        }
        self.policies = policies;
        const columns = try arena.alloc(wire.Column, self.stream.context.binding.columns.len);
        for (columns, self.stream.context.binding.columns) |*out, column| {
            try setup_owner.check();
            out.* = .{ .name = try setup_owner.copy(arena, column.name), .type = try wireType(column.type), .element_type = column.element_type, .numeric_modifier = column.numeric_modifier };
        }
        self.columns = columns;
        self.sealSetup();
        try self.runnable.park(.output_blocked);
        return .{ .context = self, .columns = columns, .next = next, .close = close, .detach = detach, .validate = validate };
    }

    fn initRetirement(self: *OwnedRead, io: std.Io) void {
        self.remote_client = null;
        self.remote_dispatch = null;
        self.retirement_io = io;
        self.root_retirement_done = .unset;
        self.stream_live = false;
        self.retirement_mutex = .unlocked;
        self.remote_owner = null;
        self.root_retirement_ready = false;
        self.root_retired = false;
    }

    fn publishSetup(self: *OwnedRead, plan: @import("../sql/plan_cache.zig").Lease, admission: http.RequestAdmission.Lease, retained_lease: *wire.RetainedLifetime.Lease) void {
        self.plan = plan;
        self.admission = admission;
        self.retained_lease = retained_lease.*;
        retained_lease.* = .{};
        // The acquisition handle cannot escape; its stable lease is owned.
        // Receipt waiting remains shared with a possible fallback while this
        // synchronous open is still running. The supervisor is gated until
        // retireSetup seals those transient borrows on every exit.
        self.authority.request.retained_lifetime = null;
        if (self.cleanup) |*reserved| reserved.publish() catch unreachable;
    }

    fn sealSetup(self: *OwnedRead) void {
        if (self.native_adapter.shared_receipt_wait_budget) |budget| self.native_adapter.receipt_wait_budget = budget.*;
        self.native_adapter.shared_receipt_wait_budget = null;
        self.authority.request.read_receipt_budget = null;
    }

    fn retireSetup(self: *OwnedRead) void {
        self.sealSetup();
        self.lifetime.close();
    }

    fn retireForFallback(self: *OwnedRead, setup_owned: *bool, request: wire.Request) !void {
        setup_owned.* = false;
        // Observe retirement while keeping this capsule and its Event alive.
        // In fail-fast mode a fallback must not race its own old SQL admission
        // lease. Timeout/cancellation retain the published supervisor/pins and
        // stop the handoff; they never certify physical retirement.
        self.lifetime.retain();
        defer self.lifetime.release();
        self.retireSetup();
        try request.check();
        const now = std.Io.Clock.Timestamp.now(request.io, request.deadline.clock);
        var until = now.addDuration(.{ .raw = .fromMilliseconds(100), .clock = request.deadline.clock });
        if (request.deadline.raw.nanoseconds < until.raw.nanoseconds) until = request.deadline;
        self.root_retirement_done.waitTimeout(request.io, .{ .deadline = until }) catch |err| {
            try request.check();
            return if (err == error.Timeout) error.SqlWriteCapacityUnavailable else err;
        };
        try request.check();
    }

    fn captureStream(self: *OwnedRead, backend: catalog.Backend, compiled: *const compiler.Compiled, parameters: []const std.json.Value, limits: @import("antfly_local_sources").sql_runtime.Limits) !bool {
        const stream = (try Pull.Stream.open(self.readAllocator(), backend, compiled, parameters, limits)) orelse return false;
        self.stream = stream;
        self.stream_live = true;
        return true;
    }

    fn prepareRemoteReads(raw: *anyopaque) !void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        if (self.remote_dispatch != null) return;
        if (self.remote_owner != null) return error.InvalidRunnableTransition;
        const client = self.remote_client orelse return error.InvalidRunnableTransition;
        // Returning an error keeps an activated-but-unready protocol closed;
        // returning null here could silently re-enable a legacy data path.
        if (!client.reads_ready) return error.RemoteReadUnavailable;
        try self.runnable.checkLifetime();
        const server = self.native_adapter.server;
        const Registrar = struct {
            server: *http.ApiHttpServer,
            pub fn bind(registrar: @This(), parent: @import("retained_read_retirement.zig").Parent) !@import("retained_read_retirement.zig").Binding {
                return registrar.server.readRetirementBinding(parent);
            }
        };
        const scheduler = if (server.runnable_owner) |active| &active.scheduler else if (server.runnable_scheduler) |*active| active else return error.CleanupCapacityUnavailable;
        self.remote_owner = try @import("remote_read_operation_owner.zig").Owner.create(server.controlAllocator(), scheduler, Registrar{ .server = server });
        errdefer {
            self.remote_owner.?.finishForeground();
            self.remote_owner = null;
        }
        try self.bindRemoteReads(client, self.remote_owner.?);
    }

    fn bindRemoteReads(self: *OwnedRead, client: @import("remote_attempt_client.zig").Client, remote: *@import("remote_read_operation_owner.zig").Owner) !void {
        // Keep the owned executor installed during worker recovery. Local
        // routes need no remote grant; the client still rejects remote
        // creation before reservation/publication while reads_ready is false.
        if (self.remote_dispatch != null) return error.InvalidRunnableTransition;
        self.remote_dispatch = try @import("retained_query_dispatch.zig").Dispatcher.initRetained(client, self, remoteDeadline, remote.retainedParent());
        remote.adoptEndpoints(&self.remote_dispatch.?);
        self.native_adapter.read_retirement = remote.binding;
        self.native_adapter.read_remote_executor = self.remote_dispatch.?.executor();
    }
    fn remoteDeadline(raw: *anyopaque) !?u64 {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        try self.runnable.checkLifetime();
        return (try self.native_adapter.context.platformDeadline()).deadline_ns;
    }
    fn lockRetirement(self: *OwnedRead) void {
        while (!self.retirement_mutex.tryLock()) std.atomic.spinLoopHint();
    }
    /// Local scans and cleanup task metadata must join before admission retires.
    /// Remote-only obligations have a separate node-owned lifetime.
    fn claimRootRetirement(self: *OwnedRead) bool {
        if (!self.root_retirement_ready or self.root_retired) return false;
        self.root_retired = true;
        return true;
    }
    fn retireRoot(self: *OwnedRead) void {
        // Only locally borrowed scans, callbacks and task metadata gate this
        // root. Remote uncertainty must not retain ordinary query admission.
        self.runnable.park(.output_blocked) catch {};
        self.lockRetirement();
        self.root_retirement_ready = true;
        const finish = self.claimRootRetirement();
        self.retirement_mutex.unlock();
        if (finish) self.finishRoot();
    }
    fn finishRoot(self: *OwnedRead) void {
        self.native_adapter.read_remote_setup = null;
        self.native_adapter.read_remote_executor = null;
        self.native_adapter.read_retirement = null;
        self.remote_client = null;
        if (self.remote_dispatch) |*dispatch| dispatch.deinit();
        self.remote_dispatch = null;
        if (self.remote_owner) |remote| remote.finishForeground();
        self.remote_owner = null;
        self.runnable.finish();
        self.admission.release();
        self.root_retirement_done.set(self.retirement_io);
    }

    pub fn validate(raw: *anyopaque, alloc: std.mem.Allocator, request: wire.Request) !void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        self.lifetime.retain();
        defer self.lifetime.release();
        var job = StreamJob{ .adapter = self.adapter, .alloc = alloc, .credential = self.authority.credential, .request = request, .owner = self, .validate_only = true };
        try job.dispatch();
    }

    fn validateCursor(self: *OwnedRead, alloc: std.mem.Allocator, request: wire.Request) !void {
        try request.check();
        try self.authority.credential.validate();
        var admission: ?http.RequestAdmission.Lease = if (self.lifetime.detached.load(.acquire) and !request.completion_drain) try self.adapter.server.acquireSqlExecutionWithOptions(false, admissionOptionsWithBytes(request, self.retained_input_bytes)) else null;
        defer if (admission) |*lease| lease.release();
        const original = self.authority.request;
        if (!std.mem.eql(u8, original.database.?, request.database orelse "default") or !std.mem.eql(u8, original.namespace.?, request.namespace orelse "public") or
            !std.meta.eql(original.search_path, request.search_path) or
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
        // StreamJob dispatch physically joins the foreground child before this
        // borrow releases, including on failure or concurrent logical detach.
        try self.lifetime.retainResource();
        defer self.lifetime.releaseResource();
        var job = StreamJob{ .adapter = self.adapter, .alloc = alloc, .credential = self.authority.credential, .request = request, .owner = self, .limit = limit };
        try job.dispatch();
        return job.page.?;
    }

    fn pull(self: *OwnedRead, alloc: std.mem.Allocator, request: wire.Request, limit: u32) !wire.StreamPage {
        const state_alloc = if (request.completion_drain) self.adapter.server.controlAllocator() else self.adapter.server.alloc;
        if (self.lifetime.detached.load(.acquire)) return error.InvalidSqlBackendResponse;
        try self.runnable.enter(if (request.completion_drain) .control else .query);
        defer self.runnable.park(.output_blocked) catch {};
        try request.check();
        // Retained snapshots have a hard initial statement deadline, even if
        // later Execute messages carry a newer per-request deadline.
        try self.authority.request.check();
        try self.authority.credential.validate();
        if (self.session_id) |id_hex| {
            const id = try @import("distributed_txn.zig").parseTxnIdHex(id_hex);
            if (try self.adapter.server.txn_sessions.principalAccess(state_alloc, id, self.authority.credential.sessionPrincipal()) != .allowed) return error.SqlTransactionNotActive;
            var state = (try self.adapter.server.txn_sessions.getSqlState(state_alloc, id)) orelse return error.SqlTransactionNotActive;
            defer state.deinit(state_alloc);
            if (state.metadata.failed or state.terminal != null) return error.SqlTransactionNotActive;
        }
        try self.validateCurrentPolicies(alloc);
        var page = try self.stream.nextBatch(limit);
        errdefer page.deinit();
        // Buffered column pages can wait on spill or catalog work too.
        try request.check();
        try self.validateCurrentPolicies(alloc);
        const owner = try alloc.create(PageOwner);
        errdefer alloc.destroy(owner);
        try self.lifetime.retainResource();
        owner.* = .{ .alloc = alloc, .page = page, .read = self };
        return .{ .exhausted = page.exhausted, .result = .{
            .columns = self.columns,
            .cells = .{ .context = owner, .count = page.values.len(), .width = self.columns.len, .read = PageOwner.cell, .read_controlled = PageOwner.cellControlled },
            .command_tag = "SELECT",
            .owner = .{ .context = owner, .release = releasePage },
        } };
    }

    fn validateCurrentPolicies(self: *OwnedRead, alloc: std.mem.Allocator) !void {
        try self.authority.request.check();
        try self.native_adapter.validateReadFence(true);
        var fresh = try self.authority.credential.identity(alloc);
        defer fresh.deinit(alloc);
        try validatePolicies(alloc, &fresh, self.identity.?, self.policies);
    }

    fn validatePolicies(alloc: std.mem.Allocator, fresh: *http.AuthenticatedIdentity, admitted: http.AuthenticatedIdentity, policies: []const Policy) !void {
        if (admitted.user_instance_id) |instance| {
            if (fresh.user_instance_id == null or !std.mem.eql(u8, &instance, &fresh.user_instance_id.?)) return error.Forbidden;
        }
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
        const read = owner.read;
        owner.page.deinit();
        owner.alloc.destroy(owner);
        read.lifetime.releaseResource();
    }

    const PageOwner = struct {
        alloc: std.mem.Allocator,
        page: Pull.BatchPage,
        read: *OwnedRead,
        fn cell(raw: *anyopaque, alloc: std.mem.Allocator, row: usize, column: usize) anyerror!wire.Cell {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return pageCell(&self.page, alloc, row, column);
        }
        fn cellControlled(raw: *anyopaque, alloc: std.mem.Allocator, row: usize, column: usize, control: ?wire.ReadControl) anyerror!wire.Cell {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (control != null) try self.read.authority.request.check();
            return pageCellControlled(&self.page, alloc, row, column, control);
        }
        // Cell conversion borrows only the page. Retained cursor lifetime
        // belongs to PageOwner and is exercised by the release callback.
        fn pageCell(page: *const Pull.BatchPage, alloc: std.mem.Allocator, row: usize, column: usize) anyerror!wire.Cell {
            return pageCellControlled(page, alloc, row, column, null);
        }
        fn pageCellControlled(page: *const Pull.BatchPage, alloc: std.mem.Allocator, row: usize, column: usize, control: ?wire.ReadControl) anyerror!wire.Cell {
            if (control) |owner| try owner.checkpoint(owner.ptr);
            if (try page.values.frozenJson(row, column)) |value| {
                if (page.columns[column].type != .json) return error.InvalidSqlBackendResponse;
                return .{ .frozen_json = value, .sql_null = false };
            }
            if (try page.values.encodedJson(row, column)) |bytes| {
                if (page.columns[column].type != .json) return error.InvalidSqlBackendResponse;
                return .{ .json_bytes = bytes, .sql_null = false };
            }
            const value = try page.values.cellControlled(alloc, row, column, control);
            if (value.array) |array| {
                const definition = page.columns[column];
                if (value.sql_null or value.value != .null or value.patterns != null or definition.type != .array or definition.element_type != array.element_type) return error.InvalidSqlBackendResponse;
                return .{ .array = array, .sql_null = false };
            }
            if (value.numeric) |number| {
                const definition = page.columns[column];
                if (value.sql_null or value.value != .null or value.patterns != null or definition.type != .number or definition.element_type != .numeric) return error.InvalidSqlBackendResponse;
                return .{ .numeric = number, .sql_null = false };
            }
            return .{ .value = if (!value.sql_null and page.columns[column].type == .datetime) try datetimeResult(alloc, value.value) else value.value, .sql_null = value.sql_null };
        }
    };

    fn detach(raw: *anyopaque) void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        self.lifetime.detach();
    }

    const cleanupCallbacks: @import("runnable_cleanup_owner.zig").Registry.Callbacks = .{
        .retain = retainCleanup,
        .release = releaseCleanup,
        .cleanup = cleanupNative,
        .retire = retireCleanup,
    };

    fn retainCleanup(raw: *anyopaque) anyerror!void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        self.lifetime.retain();
    }
    fn releaseCleanup(raw: *anyopaque) void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        if (self.cleanup_started) {
            std.debug.assert(self.physical_retired);
            // Registry releases only after its future joins and its task/slot
            // metadata physically frees. Keep the execution root through it.
            self.retireRoot();
        }
        self.lifetime.release();
    }
    fn cleanupNative(raw: *anyopaque, _: @import("antfly_runtime_abi").runnable_abi.Borrowed, _: std.Io) anyerror!void {
        retireNative(raw);
    }
    fn retireCleanup(raw: *anyopaque) anyerror!void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        // Successful cleanup is already physically retired. Failure remains
        // owned until an explicit reaper retirement request invokes this path.
        if (!self.physical_retired) retireNative(raw);
    }
    fn retirePhysical(raw: *anyopaque) void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        if (self.cleanup) |*reserved| {
            self.cleanup_started = true;
            reserved.start() catch unreachable;
            reserved.transfer() catch unreachable;
            return;
        }
        retireNative(raw);
        self.retireRoot();
    }
    fn retireNative(raw: *anyopaque) void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        std.debug.assert(!self.physical_retired);
        if (self.stream_live) {
            self.stream.close();
            self.stream_live = false;
        }
        self.native_adapter.endReadFence();
        self.plan.release(self.adapter.server.sqlPlanCacheIo());
        // Retire invocation-owned buffers before releasing its retained root.
        // Portal identity/arena storage has a separate retained lifetime.
        self.staged.deinit(self.alloc);
        self.range_guards.deinit(self.adapter.server.alloc);
        self.physical_retired = true;
    }

    fn close(raw: *anyopaque) void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        self.lifetime.close();
    }

    fn readAllocator(self: *OwnedRead) std.mem.Allocator {
        return if (self.read_memory) |*group| group.allocator() else self.alloc;
    }

    fn destroy(raw: *anyopaque) void {
        const self: *OwnedRead = @ptrCast(@alignCast(raw));
        self.identity.?.deinit(self.adapter.server.alloc);
        if (self.session_id) |id| self.adapter.server.alloc.free(id);
        self.arena.deinit();
        if (self.read_memory) |*group| group.deinit();
        Credential.release(self.authority.credential, self.adapter.server.alloc);
        var retained_lease = self.retained_lease;
        self.alloc.destroy(self);
        retained_lease.release();
    }
};

// Materializing a held cursor during COMMIT delivers an existing immutable
// page. Its statement remains SELECT; retain the internal completion authority
// for both physical worker selection and runnable CPU admission.
fn deliveryIsControl(request: wire.Request) bool {
    return request.completion_drain or @import("sql_control.zig").classify(request.statement);
}

// This consumer is independent of OwnedRead's parked producer. The row and
// allocator are borrowed only until the shared worker has physically joined.
const DeliveryJob = struct {
    adapter: *Adapter,
    credential: *Credential,
    request: wire.Request,
    work: wire.DeliveryWork,
    execution: http.RunnableScheduler.Execution = .{},
    failure: ?anyerror = null,
    done: std.Io.Event = .unset,
    fn checkpoint(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try self.request.check();
        try self.execution.checkpoint();
    }
    fn run(self: *@This()) void {
        defer self.done.set(self.request.io);
        self.runInner() catch |err| {
            self.failure = err;
        };
    }
    fn runInner(self: *@This()) !void {
        try self.request.check();
        const protected = deliveryIsControl(self.request);
        const options = admissionOptionsWithBytes(self.request, @sizeOf(DeliveryJob));
        // The result already owns its read/memory admission. Reacquiring it
        // here could wait for this very stream to retire. Worker and runnable
        // capacity bound the consumer independently of its parked producer.
        defer self.execution.finish();
        try self.adapter.server.startSqlRunnable(&self.execution, options, if (protected) .control else .query);
        try self.credential.validate();
        try self.consume();
    }
    fn consume(self: *@This()) !void {
        try checkpoint(self);
        try self.work.run(self.work.context, .{ .ptr = self, .checkpoint = checkpoint });
        try checkpoint(self);
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
        const io = if (self.owner != null and self.request.completion_drain) try self.adapter.server.sqlControlIo() else try self.adapter.server.sqlExecutionIo();
        var future = io.concurrent(run, .{self}) catch return error.SqlWriteCapacityUnavailable;
        self.done.waitUncancelable(self.request.io);
        @import("protected_future.zig").wait(io, &future);
        if (self.failure) |err| return wireFailure(self.request, err);
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
        var control = if (self.owner != null and self.request.completion_drain) try self.adapter.server.acquireSqlControl() else @as(?http.RequestAdmission.Lease, null);
        defer if (control) |*lease| lease.release();
        if (self.namespace_only) {
            const server = self.adapter.server;
            const domain = @import("antfly_local_sources").system_catalog_domain;
            const database = self.request.database orelse "default";
            const namespace = self.request.namespace orelse "public";
            try domain.validateName(database);
            try domain.validateName(namespace);
            var identity: ?http.AuthenticatedIdentity = try self.credential.identity(server.alloc);
            defer identity.?.deinit(server.alloc);
            const resource = try @import("../system_catalog/routes.zig").resourceNameAlloc(self.alloc, .{ .kind = .namespace, .database = database, .name = namespace });
            defer self.alloc.free(resource);
            if (!http.permissionsAllow(identity.?.permissions, .namespace, resource, .read)) return error.Forbidden;
            var admission = try server.acquireSqlExecutionWithOptions(false, admissionOptions(self.request));
            defer admission.release();
            var authority: Authority = .{ .credential = self.credential, .identity = &identity, .request = self.request };
            if (server.source.vtable.system_catalog == null) return error.UnsupportedSqlExecution;
            const response = try server.source.systemCatalog(self.alloc, try authority.context(), .{ .read = .{ .kind = .namespace, .database = database, .name = namespace } });
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
        var user = snapshotUser(alloc, manager, username) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.Unauthorized,
        };
        defer user.deinit(alloc);
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

    /// Own the authenticated capsule without retaining a connection allocator,
    /// password or verifier hash. UserManager remains service-owned and must
    /// outlive every capsule; validate/identity continue to read live authority.
    fn cloneOwned(self: *const Credential, alloc: std.mem.Allocator) !*Credential {
        try self.validate();
        const copy = try alloc.create(Credential);
        errdefer alloc.destroy(copy);
        const name = try alloc.dupe(u8, self.username);
        errdefer alloc.free(name);
        const principal = try alloc.dupe(u8, self.principal);
        errdefer alloc.free(principal);
        copy.* = .{ .manager = self.manager, .username = name, .principal = principal, .verifier_mac = self.verifier_mac };
        errdefer std.crypto.secureZero(u8, &copy.verifier_mac);
        // Rotation during allocation must not publish a stale credential.
        try copy.validate();
        return copy;
    }

    pub fn validate(self: *const Credential) !void {
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
        var snapshot = try self.manager.snapshotCredentialPoliciesAlloc(alloc, self.principal);
        defer snapshot.deinit(alloc);
        const result = (try http.cloneCatalogIdentity(alloc, .{
            .user_instance_id = snapshot.instance_id,
            .username = snapshot.policy.username,
            .credential_principal = self.principal,
            .permissions = snapshot.policy.permissions,
            .row_filter = snapshot.policy.row_filter,
            .metadata_json = snapshot.policy.metadata_json,
            .roles = snapshot.policy.roles,
            .live_user_manager = self.manager,
        })).?;
        var owned = result;
        errdefer owned.deinit(alloc);
        try self.validate();
        return owned;
    }
};

fn snapshotUser(alloc: std.mem.Allocator, manager: *usermgr.UserManager, username: []const u8) !usermgr.User {
    var lease = try manager.acquireSnapshotLease();
    defer lease.release();
    return lease.getUserAlloc(alloc, username);
}

fn validateRequest(request: wire.Request) !void {
    try request.check();
    if (request.deadline.clock != .awake) return error.UnsupportedSqlExecution;
    if (request.limit == 0 or request.limit > 4096 or request.parameters.len > 1024) return error.InvalidSqlParameters;
}

/// Materialized rows can outlive the worker stack. Keep their allocator group
/// at a stable address until the wire result owner physically releases them.
const MaterializedRead = struct {
    parent: std.mem.Allocator,
    memory: @import("../common/read_memory.zig").Group,
    result: ?@import("antfly_local_sources").sql_runtime.Result = null,

    fn create(parent: std.mem.Allocator, pool: *@import("../common/read_memory.zig").Pool) !*MaterializedRead {
        const self = try parent.create(MaterializedRead);
        errdefer parent.destroy(self);
        self.* = .{ .parent = parent, .memory = try @import("../common/read_memory.zig").Group.init(pool, .general, pool.limits.bytes[0]) };
        return self;
    }

    fn release(raw: *anyopaque) void {
        const self: *MaterializedRead = @ptrCast(@alignCast(raw));
        if (self.result) |*result| result.deinit();
        self.memory.deinit();
        const parent = self.parent;
        parent.destroy(self);
    }
};

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
        const protected = @import("sql_control.zig").classify(self.request.statement);
        const preparation_owner = if (protected) &self.adapter.server.sql_control_preparation else &self.adapter.server.sql_preparation_admission;
        var preparation = preparation_owner.tryAcquireLease() orelse return error.SqlWriteCapacityUnavailable;
        defer preparation.release();
        var preparation_runnable: http.RunnableScheduler.Execution = .{};
        defer preparation_runnable.finish();
        const preparation_options = admissionOptions(self.request);
        try self.adapter.server.startSqlRunnable(&preparation_runnable, preparation_options, if (protected) .control else .query);
        var preparation_scope: @import("sql_preparation.zig").Owner = .{ .execution = &preparation_runnable, .options = preparation_options };
        var lease = @import("sql_control.zig").Plan.acquireControlled(self.alloc, self.adapter.server.sqlPlanCache(), self.adapter.server.sqlPlanCacheIo(), .{
            .statement = self.request.statement,
            .principal = self.credential.principal,
            .database = self.request.database orelse "default",
            .namespace = self.request.namespace orelse "public",
        }, &self.compile_diagnostic, protected, preparation_scope.control()) catch |err| {
            self.compile_diagnostic_valid = @import("sql_preparation.zig").hasCompilerDiagnostic(err);
            return preparation_scope.original(err);
        };
        defer lease.release(self.adapter.server.sqlPlanCacheIo());
        const compiled = lease.compiled();
        const write = self.kind == .execute and switch (compiled.statement) {
            .select, .explain => false,
            .insert, .update, .delete, .merge, .create_table, .drop_table, .catalog_ddl, .policy_ddl, .begin, .commit, .rollback, .savepoint, .rollback_to_savepoint, .release_savepoint, .set_constraints => true,
        };
        const server = self.adapter.server;
        preparation.release();
        if (!write) try @import("sql_read_lane.zig").apply(&preparation_runnable, compiled);
        var admission = try server.acquireSqlExecutionUnderRunnable(&preparation_runnable, write, protected, preparation_options);
        defer admission.release();
        // The preparation root stays at its original address through execution
        // and plan retirement, including accepted durable write completion.
        const runnable = &preparation_runnable;
        const identity_alloc = if (protected) server.controlAllocator() else server.alloc;
        var identity: ?http.AuthenticatedIdentity = try self.credential.identity(identity_alloc);
        defer identity.?.deinit(identity_alloc);
        var authority = Authority{ .credential = self.credential, .identity = &identity, .request = self.request };
        const context = try authority.context();
        // Stable local completion receivers outlive the root's deferred finish.
        var write_completion: ?@import("runnable_write_team.zig").Completion = null;
        if (write) {
            if (runnable.entry) |entry| {
                const memory = server.request_memory orelse return error.ExecutionWaitUnsupported;
                write_completion = try @import("runnable_write_team.zig").Completion.init(runnable, memory.allocator(entry.class == .control));
            }
        }
        var native_adapter = execution.Adapter{
            .runnable = if ((!write or protected) and runnable.entry != null) runnable else null,
            .write_execution = if (write and runnable.entry != null) runnable else null,
            .write_completion = if (write_completion) |*bridge| bridge else null,
            .serial_execution = runnable.entry != null,
            .server = server,
            .identity = &identity,
            .context = context,
            .database = self.request.database orelse "default",
            .namespace = self.request.namespace orelse "public",
            .read_requirements = self.request.read_requirements,
            .shared_receipt_wait_budget = self.request.read_receipt_budget,
            .lake_visibility = @fromBackingInt(@intCast(@backingInt(self.request.lake_visibility))),
            .ddl_search_path = if (self.request.search_path) |*path| path else null,
            .session_id = self.request.session_id,
            .session_namespace = self.request.session_namespace,
            .setting_overlay = self.request.setting_overlay,
            .setting_overlay_source = .connection,
            .expected_setting_epoch = self.request.setting_epoch,
        };
        // describe/execute capture a single settings view. The adapter's
        // loader checks expected_setting_epoch on that exact captured epoch.
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
            var describe_backend = guarded.backend();
            describe_backend.parameter_descriptor_hints = self.request.parameter_descriptors;
            if (compiled.uses_current_setting) describe_backend.setting_capture = native_adapter.settingCapture();
            var description = try describe_sql.describe(self.request.scratch_allocator orelse self.alloc, describe_backend, compiled, hints);
            defer description.deinit();
            const columns = try self.alloc.alloc(wire.Column, description.binding.columns.len);
            for (columns, description.binding.columns) |*out, column| out.* = .{ .name = try self.alloc.dupe(u8, column.name), .type = try wireType(column.type), .element_type = column.element_type, .numeric_modifier = column.numeric_modifier };
            const parameter_types = try self.alloc.alloc(wire.Type, description.binding.parameter_types.len);
            for (parameter_types, description.binding.parameter_types) |*out, kind| out.* = if (kind) |value| try wireType(value) else .unknown;
            self.description = .{
                .columns = columns,
                .parameter_types = parameter_types,
                .parameter_descriptors = try self.alloc.dupe(wire.Parameter, description.binding.parameter_descriptors),
                .binding_guard = try statementBindingGuard(self.alloc, native_adapter.revision, description.binding),
                .setting_epoch = if (description.settings) |view| view.epoch else null,
            };
            return;
        }
        var normalization_owner: @import("sql_preparation.zig").Owner = .{ .execution = runnable, .options = preparation_options };
        const parameters = if (self.request.parameter_cells != null) self.request.parameters else try normalizeParametersControlled(self.alloc, self.request.parameters, self.request.parameter_types, &normalization_owner);
        var execution_backend = guarded.backend();
        execution_backend.parameter_descriptor_hints = self.request.parameter_descriptors;
        var transferred = false;
        const read_owner = if (!write) if (server.readMemoryPool()) |pool| try MaterializedRead.create(self.alloc, pool) else null else null;
        defer if (!transferred) if (read_owner) |owner| MaterializedRead.release(owner);
        const execution_alloc = if (read_owner) |owner| owner.memory.allocator() else self.request.scratch_allocator orelse self.alloc;
        var result = native_adapter.execute(execution_alloc, compiled, parameters, .{ .result_rows = self.request.limit, .native_result = true, .parameter_cells = self.request.parameter_cells, .parameter_owner = self.request.parameter_owner }, execution_backend) catch |err| {
            if (self.request.diagnostics) |diagnostic| diagnostic.transaction_status = @fromBackingInt(@backingInt(native_adapter.transaction_status));
            if (err == error.SqlMutationOutcomeUnknown or err == error.SqlTransactionOutcomeUnknown or err == error.SessionLeaseLost) if (self.request.diagnostics) |diagnostic|
                diagnostic.set("40003", "transaction outcome is unknown; do not replay this statement", native_adapter.outcome_transaction_id, false);
            return err;
        };
        defer if (!transferred) result.deinit();
        if (self.request.diagnostics) |diagnostic| diagnostic.transaction_status = @fromBackingInt(@backingInt(native_adapter.transaction_status));
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
        for (columns, result.output.columns) |*out, column| out.* = .{ .name = column.name, .type = try wireType(column.type), .element_type = column.element_type, .numeric_modifier = column.numeric_modifier };
        const native_owner = if (result.output.native_rows != null) try result.state.arena.allocator().create(NativeResultCells) else null;
        if (native_owner) |owner| owner.* = .{ .result = result };
        self.result = .{
            .columns = columns,
            .cells = if (native_owner) |owner| .{ .context = owner, .count = result.output.rowCount(), .width = columns.len, .read = NativeResultCells.read } else null,
            .rows = result.output.rows,
            .sql_nulls = result.output.sql_nulls,
            .rows_affected = result.output.rows_affected,
            .command_tag = try commandTag(self.alloc, result.output.command_tag, result.output.rowCount(), result.output.rows_affected),
            .mutation_outcome = if (result.output.mutation_outcome) |outcome| switch (outcome) {
                inline else => |kind| @field(wire.MutationOutcome, @tagName(kind)),
            } else null,
            .transaction_id = native_adapter.outcome_transaction_id,
            .ddl_receipt_json = if (result.output.ddl_receipt) |receipt| try std.json.Stringify.valueAlloc(self.alloc, receipt, .{ .emit_null_optional_fields = false }) else null,
            .session_id = if (native_adapter.result_session_id) |id| try self.alloc.dupe(u8, &id) else null,
            .transaction_status = @fromBackingInt(@backingInt(native_adapter.transaction_status)),
            .owner = if (read_owner) |owner| .{ .context = owner, .release = MaterializedRead.release } else .{ .context = result.state, .release = releaseResult },
        };
        if (read_owner) |owner| owner.result = result;
        transferred = true;
    }
};

test "SQL pgwire dispatch preserves imported executor authority including unavailable views" {
    const Probe = struct {
        attempts: usize = 0,
        fn concurrent(raw: ?*anyopaque, _: usize, _: std.mem.Alignment, _: []const u8, _: std.mem.Alignment, _: *const fn (*const anyopaque, *anyopaque) void) std.Io.ConcurrentError!*std.Io.AnyFuture {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.attempts += 1;
            return error.ConcurrencyUnavailable;
        }
    };
    var raw_probe: Probe = .{};
    var imported_probe: Probe = .{};
    var vtable = std.Io.failing.vtable.*;
    vtable.concurrent = Probe.concurrent;
    const raw_io: std.Io = .{ .userdata = &raw_probe, .vtable = &vtable };
    const imported_io: std.Io = .{ .userdata = &imported_probe, .vtable = &vtable };
    var runtime = try @import("antfly_local_sources").storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{
        .backend = .manual,
        .borrowed_io = .{ .general = raw_io },
    });
    defer runtime.deinit();
    // Dispatch must reject or schedule before dereferencing either object.
    var manager: usermgr.UserManager = undefined;
    var credential: Credential = undefined;
    credential.manager = &manager;
    var api: http.ApiHttpServer = undefined;
    api.cfg = .{ .backend_runtime = runtime.ptr(), .user_manager = &manager, .imported_runtime_io = .{} };
    var adapter: Adapter = .{ .server = &api };
    var canceled: std.atomic.Value(bool) = .init(false);
    const request: wire.Request = .{
        .statement = "SELECT id FROM docs",
        .limit = 1,
        .io = std.testing.io,
        .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } },
        .cancel_requested = &canceled,
    };
    for ([_]?std.Io{ null, imported_io }, 0..) |executor, i| {
        api.cfg.imported_runtime_io.?.durable = raw_io;
        api.cfg.imported_runtime_io.?.sql_execution = executor;
        api.cfg.imported_runtime_io.?.sql_control = executor;
        var job: Job = .{ .adapter = &adapter, .alloc = std.testing.allocator, .credential = &credential, .request = request, .kind = .execute };
        try std.testing.expectError(error.SqlWriteCapacityUnavailable, adapter.dispatch(&job));
        var stream: StreamJob = .{ .adapter = &adapter, .alloc = std.testing.allocator, .credential = &credential, .request = request };
        try std.testing.expectError(error.SqlWriteCapacityUnavailable, stream.dispatch());
        try std.testing.expectEqual(@as(usize, 0), raw_probe.attempts);
        var control_request = request;
        control_request.statement = "ROLLBACK";
        var control_job: Job = .{ .adapter = &adapter, .alloc = std.testing.allocator, .credential = &credential, .request = control_request, .kind = .execute };
        try std.testing.expectError(error.SqlWriteCapacityUnavailable, adapter.dispatch(&control_job));
        try std.testing.expectEqual(@as(usize, 0), raw_probe.attempts);
        try std.testing.expectEqual(i * 3, imported_probe.attempts);
    }

    // A held cursor keeps its SELECT text while COMMIT drains its page. The
    // consumer must use imported control IO, while fresh SELECT and mixed
    // statements still use the ordinary execution owner.
    var control_probe: Probe = .{};
    const control_io: std.Io = .{ .userdata = &control_probe, .vtable = &vtable };
    api.cfg.imported_runtime_io.?.sql_execution = imported_io;
    api.cfg.imported_runtime_io.?.sql_control = control_io;
    const identity: wire.Identity = .{ .context = &credential, .release = Credential.release };
    const Never = struct {
        fn run(_: *anyopaque, _: ?wire.ReadControl) !void {
            return error.TestUnexpectedResult;
        }
    };
    const work: wire.DeliveryWork = .{ .context = &raw_probe, .run = Never.run };
    const execution_before = imported_probe.attempts;
    try std.testing.expectError(error.SqlWriteCapacityUnavailable, Adapter.deliverRow(&adapter, identity, request, work));
    var drain = request;
    drain.completion_drain = true;
    try std.testing.expectError(error.SqlWriteCapacityUnavailable, Adapter.deliverRow(&adapter, identity, drain, work));
    var rollback = request;
    rollback.statement = "ROLLBACK";
    try std.testing.expectError(error.SqlWriteCapacityUnavailable, Adapter.deliverRow(&adapter, identity, rollback, work));
    var mixed = request;
    mixed.statement = "COMMIT; SELECT 1";
    try std.testing.expectError(error.SqlWriteCapacityUnavailable, Adapter.deliverRow(&adapter, identity, mixed, work));
    try std.testing.expectEqual(execution_before + 2, imported_probe.attempts);
    try std.testing.expectEqual(@as(usize, 2), control_probe.attempts);
    try std.testing.expectEqual(@as(usize, 0), raw_probe.attempts);
}

test "SQL pgwire native execute arguments use bounded scalar semantics without table access" {
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
    var array_request = request;
    array_request.parameter_types = &.{ .array, .integer };
    array_request.parameter_descriptors = &.{ .{ .kind = .array, .element_type = .int64 }, .{ .kind = .integer, .element_type = .int32 } };
    const array_values = try Adapter.evaluateScalarParameters(arena.allocator(), array_request, &.{ "'[-1:1]={9007199254740993,NULL,2}'::bigint[]", "42" });
    var decoded = try @import("antfly_local_sources").sql_array_wire.decode(std.testing.allocator, .int64, array_values[0], .{});
    defer decoded.deinit();
    try std.testing.expectEqual(@as(i32, -1), decoded.value.dimensions[0].lower);
    try std.testing.expectEqual(@as(i64, 9007199254740993), decoded.value.elements[0].value.integer);
    try std.testing.expect(decoded.value.elements[1].sql_null);
    const native_array = try Adapter.evaluateScalarCells(arena.allocator(), array_request, &.{ "'[-1:1]={9007199254740993,NULL,2}'::bigint[]", "42" });
    try std.testing.expectEqual(@as(i64, 9007199254740993), native_array[0].array.?.flatCell(0).value.integer);
    try std.testing.expect(native_array[0].array.?.flatCell(1).sql_null);
    const native_json = try Adapter.evaluateScalarCells(arena.allocator(), json_request, &.{ "'null'::json", "NULL::json" });
    try std.testing.expect(!native_json[0].sql_null and native_json[1].sql_null);
    canceled.store(true, .release);
    try std.testing.expectError(error.QueryCanceled, Adapter.evaluateScalarParameters(arena.allocator(), request, &.{ "1", "'x'" }));
}

fn normalizeParameters(alloc: std.mem.Allocator, input: []const std.json.Value, types: []const wire.Type) ![]const std.json.Value {
    return normalizeParametersControlled(alloc, input, types, null);
}

fn normalizeParametersControlled(alloc: std.mem.Allocator, input: []const std.json.Value, types: []const wire.Type, owner: ?*@import("sql_preparation.zig").Owner) ![]const std.json.Value {
    if (input.len != types.len) return error.InvalidSqlParameters;
    var normalized: ?[]std.json.Value = null;
    var converted_until: usize = 0;
    errdefer if (normalized) |values| {
        // Only converted datetime cells own new strings; all other cells are
        // borrowed from the original parameter capsule.
        for (input[0..converted_until], types[0..converted_until], 0..) |original, kind, index| {
            if (kind == .datetime and original != .null and original != .string)
                alloc.free(values[index].string);
        }
        alloc.free(values);
    };
    for (input, types, 0..) |value, kind, index| {
        if (owner) |preparation| try preparation.check();
        if (kind != .datetime or value == .null or value == .string) continue;
        if (normalized == null) normalized = if (owner) |preparation| try preparation.copyElements(std.json.Value, alloc, input) else try alloc.dupe(std.json.Value, input);
        const nanos = if (owner != null and value == .number_string)
            try owner.?.unsignedDecimal(value.number_string)
        else
            try wire_values.timestampNanos(value);
        normalized.?[index] = .{ .string = try storage_schema.formatDateTimeNsAlloc(alloc, nanos) };
        converted_until = index + 1;
    }
    if (owner) |preparation| try preparation.check();
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

fn wireType(kind: ast.ColumnType) !wire.Type {
    return switch (kind) {
        inline else => |tag| @field(wire.Type, @tagName(tag)),
    };
}

fn commandTag(alloc: std.mem.Allocator, command: []const u8, rows: usize, affected: u64) ![]const u8 {
    if (std.mem.eql(u8, command, "DDL PENDING")) return alloc.dupe(u8, command);
    if (std.mem.eql(u8, command, "SELECT")) return std.fmt.allocPrint(alloc, "SELECT {d}", .{rows});
    if (std.mem.eql(u8, command, "INSERT")) return std.fmt.allocPrint(alloc, "INSERT 0 {d}", .{affected});
    if (std.mem.eql(u8, command, "UPDATE") or std.mem.eql(u8, command, "DELETE") or std.mem.eql(u8, command, "MERGE")) return std.fmt.allocPrint(alloc, "{s} {d}", .{ command, affected });
    if (std.mem.startsWith(u8, command, "CREATE ") or std.mem.startsWith(u8, command, "DROP ") or std.mem.startsWith(u8, command, "ALTER ")) return alloc.dupe(u8, command);
    inline for (.{ "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "SET CONSTRAINTS", "TRUNCATE TABLE" }) |tag| if (std.mem.eql(u8, command, tag)) return alloc.dupe(u8, command);
    return error.UnsupportedSqlExecution;
}

test "SQL pgwire MERGE completion reports committed affected rows" {
    const tag = try commandTag(std.testing.allocator, "MERGE", 0, 3);
    defer std.testing.allocator.free(tag);
    try std.testing.expectEqualStrings("MERGE 3", tag);
}

test "SQL pgwire native materialized cells match public text and binary encodings" {
    const Probe = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn run(a: std.mem.Allocator) !void {
            var marker: u8 = 0;
            const backend: catalog.Backend = .{ .ptr = &marker, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = checkpoint } };
            var compiled = try compiler.compile(a, "SELECT ARRAY[9007199254740993::bigint,NULL::bigint,-7::bigint] AS a, '9007199254740993.1200'::numeric AS n, 'null'::jsonb AS j, NULL::numeric AS z", .{});
            defer compiled.deinit();
            var result = try native.execute(a, backend, &compiled, &.{}, .{ .native_result = true });
            defer result.deinit();
            var baseline = try native.execute(a, backend, &compiled, &.{}, .{});
            defer baseline.deinit();
            var owner: NativeResultCells = .{ .result = result };
            var columns: [4]wire.Column = undefined;
            for (&columns, result.output.columns) |*out, column| out.* = .{ .name = column.name, .type = try wireType(column.type), .element_type = column.element_type };
            const published: wire.Result = .{ .columns = &columns, .cells = .{ .context = &owner, .count = result.output.rowCount(), .width = columns.len, .read = NativeResultCells.read }, .command_tag = "SELECT 1" };
            const view = try published.view(0, 1);
            for ([_]u16{ 0, 1 }) |format| for (columns, 0..) |column, index| {
                const cell = try view.cell(a, 0, index);
                const expected_null = baseline.output.sql_nulls.?[0][index];
                try std.testing.expectEqual(expected_null, cell.sql_null);
                if (cell.sql_null) continue;
                var bytes: std.Io.Writer.Allocating = .init(a);
                defer bytes.deinit();
                var expected: std.Io.Writer.Allocating = .init(a);
                defer expected.deinit();
                wire_values.encodeCellInto(a, &bytes.writer, column, format, cell, 1 << 20) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
                wire_values.encodeColumnInto(a, &expected.writer, column, format, baseline.output.rows[0][index], 1 << 20) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
                try std.testing.expectEqualSlices(u8, expected.written(), bytes.written());
            };
            try std.testing.expectError(error.InvalidResult, published.cell(a, 1, 0));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

const NativeResultCells = struct {
    result: native.Result,
    fn read(raw: *anyopaque, alloc: std.mem.Allocator, row: usize, column: usize) !wire.Cell {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const rows = self.result.output.native_rows orelse return error.InvalidSqlBackendResponse;
        if (row >= rows.len or column >= self.result.output.columns.len or rows[row].len != self.result.output.columns.len) return error.InvalidSqlBackendResponse;
        const datum = rows[row][column];
        return .{ .value = if (!datum.sql_null and self.result.output.columns[column].type == .datetime) try datetimeResult(alloc, datum.value) else datum.value, .sql_null = datum.sql_null, .array = datum.array, .numeric = datum.numeric };
    }
};

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
        var result = try nativeRequestContext(&self.request);
        result.principal = .{ .kind = .user, .subject = self.identity.*.?.username };
        result.destination_authorization_principal = self.credential.principal;
        // The identity is borrowed only for this statement. Owner write proofs
        // are minted before a durable transaction plan is sealed.
        result.row_policy_credential = self.identity;
        result.table_write_authorization = .{ .ptr = self, .allows = allowsWrite };
        return result;
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
    cleanup: ?*http.RunnableScheduler.Execution = null,
    fn backend(self: *GuardedCatalog) catalog.Backend {
        var result = self.native;
        result.ptr = self;
        result.vtable = &.{ .resolve = resolve, .scan = scan, .supports_scan_order = true, .open_scan = openScan, .open_statement = openStatement, .mutate = mutate, .mutate_prepared = mutatePrepared, .prepare_mutations = prepareMutations, .resolve_conflict_owners = resolveConflictOwners, .generate_row_id = generateRowId, .checkpoint = checkpoint, .ddl = ddl };
        return result;
    }
    fn generateRowId(raw: *anyopaque, alloc: std.mem.Allocator) ![]const u8 {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        const generate = self.native.vtable.generate_row_id orelse return error.UnsupportedSqlExecution;
        return generate(self.native.ptr, alloc);
    }
    fn resolveConflictOwners(raw: *anyopaque, alloc: std.mem.Allocator, scratch_alloc: std.mem.Allocator, table: catalog.Table, target: catalog.ConflictTarget, mutations: []const catalog.Mutation) ![]const catalog.ConflictOwner {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try self.checkRead(table.physical_name);
        if (!Authority.allowsWrite(self.authority, table.physical_name)) return error.Forbidden;
        const resolve_owners = self.native.vtable.resolve_conflict_owners orelse return error.UnsupportedSqlShape;
        return resolve_owners(self.native.ptr, alloc, scratch_alloc, table, target, mutations);
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        if (self.cleanup) |execution_owner| if (execution_owner.entry) |entry| if (entry.cleanup_active) {
            // Logical detach fenced foreground readers before activation.
            // Physical cleanup uses the authorized native deadline, including
            // after request cancellation or credential revocation.
            try self.native.vtable.checkpoint(self.native.ptr);
            return;
        };
        try self.authority.checkpoint();
        try self.native.vtable.checkpoint(self.native.ptr);
        // A native checkpoint may relinquish execution and wait. Authority
        // checked before that wait cannot authorize the resumed operation.
        try self.authority.checkpoint();
        try self.authority.credential.validate();
    }
    fn resolve(raw: *anyopaque, alloc: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        try self.authority.credential.validate();
        const table = try resolveOnPath(self.native, alloc, name, action, if (self.authority.request.search_path) |*path| path else null);
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
        fn nextColumns(raw: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.ColumnPage {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.guard.checkRead(self.table);
            return self.inner.next_columns.?(self.inner.ptr, alloc, limit);
        }
        fn countRows(raw: *anyopaque) !?u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.guard.checkRead(self.table);
            return self.inner.count_rows.?(self.inner.ptr);
        }
        fn wantsExactFilter(raw: *anyopaque, columns: []const @import("antfly_local_sources").sql_dynamic_filter.Column) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.guard.checkRead(self.table);
            return self.inner.wants_exact_filter.?(self.inner.ptr, columns);
        }
        fn setDynamicFilter(raw: *anyopaque, filter: *const @import("antfly_local_sources").sql_dynamic_filter.Filter) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.guard.checkRead(self.table);
            return self.inner.set_dynamic_filter.?(self.inner.ptr, filter);
        }
        fn splitScan(raw: *anyopaque, alloc: std.mem.Allocator, workers: usize) !?[]catalog.Cursor {
            return splitImpl(raw, alloc, workers, false);
        }
        fn splitOrdered(raw: *anyopaque, alloc: std.mem.Allocator, workers: usize) !?[]catalog.Cursor {
            return splitImpl(raw, alloc, workers, true);
        }
        fn splitImpl(raw: *anyopaque, alloc: std.mem.Allocator, workers: usize, ordered: bool) !?[]catalog.Cursor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.guard.checkRead(self.table);
            const split = if (ordered) self.inner.split_ordered.? else self.inner.split_scan.?;
            const children = (try split(self.inner.ptr, alloc, workers)) orelse return null;
            defer alloc.free(children);
            var wrapped: usize = 0;
            errdefer for (children[wrapped..]) |child| child.close(child.ptr);
            const cursors = try alloc.alloc(catalog.Cursor, children.len);
            errdefer alloc.free(cursors);
            errdefer for (cursors[0..wrapped]) |child| child.close(child.ptr);
            for (children, cursors) |child, *out| {
                const name = try alloc.dupe(u8, self.table);
                errdefer alloc.free(name);
                const owner = try alloc.create(ReadCursor);
                owner.* = .{ .alloc = alloc, .guard = self.guard, .table = name, .inner = child };
                out.* = owner.cursor(close);
                wrapped += 1;
            }
            return cursors;
        }
        fn cursor(self: *@This(), release: *const fn (*anyopaque) void) catalog.Cursor {
            return .{
                .column_statistics = self.inner.column_statistics,
                .estimated_filtered_rows = self.inner.estimated_filtered_rows,
                .estimated_rows = self.inner.estimated_rows,
                .estimated_bytes = self.inner.estimated_bytes,
                .ptr = self,
                .next = next,
                .close = release,
                .next_columns = if (self.inner.next_columns != null) nextColumns else null,
                .count_rows = if (self.inner.count_rows != null) countRows else null,
                .set_dynamic_filter = if (self.inner.set_dynamic_filter != null) setDynamicFilter else null,
                .wants_exact_filter = if (self.inner.wants_exact_filter != null) wantsExactFilter else null,
                .split_scan = if (self.inner.split_scan != null) splitScan else null,
                .split_ordered = if (self.inner.split_ordered != null) splitOrdered else null,
            };
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
        return cursor.cursor(ReadCursor.close);
    }
    fn mutate(raw: *anyopaque, alloc: std.mem.Allocator, scratch_alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        if (!Authority.allowsWrite(self.authority, table.physical_name)) return error.Forbidden;
        return self.native.vtable.mutate(self.native.ptr, alloc, scratch_alloc, table, input);
    }

    fn mutatePrepared(raw: *anyopaque, alloc: std.mem.Allocator, scratch_alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        if (!Authority.allowsWrite(self.authority, table.physical_name)) return error.Forbidden;
        const commit = self.native.vtable.mutate_prepared orelse return error.UnsupportedSqlExecution;
        return commit(self.native.ptr, alloc, scratch_alloc, table, input);
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
            out.* = wrapper.cursor(StatementRead.borrowedClose);
        }
        owner.* = .{ .alloc = alloc, .inner = inner, .wrappers = wrappers, .cursors = cursors };
        return .{ .ptr = owner, .cursors = cursors, .close = StatementRead.close };
    }

    fn ddl(raw: *anyopaque, allocators: catalog.DdlAllocators, input: catalog.Ddl) !catalog.DdlOutcome {
        const self: *GuardedCatalog = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        const dispatch_ddl = self.native.vtable.ddl orelse return error.UnsupportedSqlExecution;
        // Refresh current grants before native DDL authorizes resource, rename
        // destination and tablespace. Prepared metadata never grants authority.
        var current = try self.authority.credential.identity(allocators.scratch);
        defer current.deinit(allocators.scratch);
        const prior = self.authority.identity.*;
        self.authority.identity.* = current;
        defer self.authority.identity.* = prior;
        return dispatch_ddl(self.native.ptr, allocators, input);
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
    if (binding.relation) |relation| {
        // Relational binding.table describes a virtual projection, not a
        // catalog input. Its scans retain the actual physical identities.
        for (relation.scans) |scan| try collectBindingTable(scan.table, scans, count);
    } else if (binding.table) |table| try collectBindingTable(table, scans, count);
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

test "SQL pgwire relational binding guard omits virtual projection identities without catalog reads" {
    const Backend = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            return error.TestUnexpectedCatalogRead;
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.TestUnexpectedRowRead;
        }
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.TestUnexpectedMutation;
        }
    };
    const a = std.testing.allocator;
    var compiled = try compiler.compile(a, "SELECT $1 AS x UNION ALL SELECT $1 + 1", .{});
    defer compiled.deinit();
    var marker: u8 = 0;
    var description = try describe_sql.describe(a, .{ .ptr = &marker, .parameter_descriptor_hints = &.{.{ .kind = .integer, .element_type = .int64 }}, .vtable = &.{ .resolve = Backend.resolve, .scan = Backend.scan, .mutate = Backend.mutate, .checkpoint = Backend.checkpoint } }, &compiled, &.{.integer});
    defer description.deinit();
    try std.testing.expect(description.binding.relation != null);
    try std.testing.expectEqual(@as(usize, 0), description.binding.relation.?.scans.len);
    try std.testing.expectEqual(@as(usize, 1), description.binding.parameter_types.len);
    try std.testing.expectEqual(ast.ColumnType.integer, description.binding.parameter_types[0].?);
    // A projection schema is not an input identity. This statement needs no
    // catalog revision and must not fabricate a prepared-table guard.
    const guard = try statementBindingGuard(a, null, description.binding);
    defer if (guard) |value| a.free(value);
    try std.testing.expect(guard == null);
}

test "SQL pgwire relational binding guard retains every physical input and requires its revision" {
    const Backend = struct {
        fn table(left: bool) catalog.Table {
            return .{ .id = if (left) 41 else 42, .physical_name = if (left) "storage_left" else "storage_right", .schema_version = if (left) 3 else 5, .columns = &.{.{ .name = "id", .path = "id", .type = .integer, .element_type = .int64, .nullable = false }} };
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn resolve(_: *anyopaque, _: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
            if (action != .read) return error.TestUnexpectedMutation;
            if (std.mem.eql(u8, name.table, "left_table")) return table(true);
            if (std.mem.eql(u8, name.table, "right_table")) return table(false);
            return error.TestUnexpectedCatalogRead;
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.TestUnexpectedRowRead;
        }
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.TestUnexpectedMutation;
        }
    };
    const a = std.testing.allocator;
    var compiled = try compiler.compile(a, "SELECT l.id FROM left_table l JOIN right_table r ON l.id = r.id", .{});
    defer compiled.deinit();
    var marker: u8 = 0;
    var description = try describe_sql.describe(a, .{ .ptr = &marker, .vtable = &.{ .resolve = Backend.resolve, .scan = Backend.scan, .mutate = Backend.mutate, .checkpoint = Backend.checkpoint } }, &compiled, &.{});
    defer description.deinit();
    try std.testing.expectEqual(@as(usize, 2), description.binding.relation.?.scans.len);
    try std.testing.expectError(error.InvalidSqlBackendResponse, statementBindingGuard(a, null, description.binding));
    const guard = (try statementBindingGuard(a, 17, description.binding)).?;
    defer a.free(guard);
    try std.testing.expectEqual(@as(u16, 2), std.mem.readInt(u16, guard[8..10], .little));
    for ([_]catalog.Table{ Backend.table(true), Backend.table(false) }) |table| {
        try verifyBindingGuard(guard, 17, table);
        var replaced = table;
        replaced.id += 100;
        try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard(guard, 17, replaced));
        replaced = table;
        replaced.schema_version += 1;
        try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard(guard, 17, replaced));
        try std.testing.expectError(error.CatalogGenerationChanged, verifyBindingGuard(guard, 18, table));
    }
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
    var joined: @import("antfly_local_sources").sql_joined_mutation.Bound = undefined;
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
            // Exercise the real manager lock while the caller bounds all
            // transient credential data; no copied mutex or manager allocator.
            var snapshot = try snapshotUser(failing, original, "alice");
            defer snapshot.deinit(failing);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, SnapshotAllocation.check, .{&manager});
    try std.testing.expectError(error.Unauthorized, Credential.authenticate(alloc, &manager, "missing", "secret"));
    try std.testing.expectError(error.InvalidPassword, Credential.authenticate(alloc, &manager, "alice", "wrong"));
    const credential = try Credential.authenticate(alloc, &manager, "alice", "secret");
    defer Credential.release(credential, alloc);
    const CloneAllocation = struct {
        fn check(failing: std.mem.Allocator, original: *Credential) !void {
            const copy = try original.cloneOwned(failing);
            defer Credential.release(copy, failing);
            try std.testing.expect(copy.username.ptr != original.username.ptr);
            try std.testing.expect(copy.principal.ptr != original.principal.ptr);
            try std.testing.expectEqualStrings(original.username, copy.username);
            try std.testing.expectEqualStrings(original.principal, copy.principal);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, CloneAllocation.check, .{credential});
    const retained_credential = retained: {
        var connection_arena = std.heap.ArenaAllocator.init(alloc);
        defer connection_arena.deinit();
        const connection_alloc = connection_arena.allocator();
        const connection_credential = try credential.cloneOwned(connection_alloc);
        defer Credential.release(connection_credential, connection_alloc);
        break :retained try connection_credential.cloneOwned(alloc);
    };
    defer Credential.release(retained_credential, alloc);
    // The connection credential and its allocator are gone; the retained
    // capsule still owns its names and retains the original verifier fence.
    try retained_credential.validate();
    try std.testing.expectEqualStrings("alice", retained_credential.sessionPrincipal());
    var retained_identity = try retained_credential.identity(alloc);
    defer retained_identity.deinit(alloc);
    try std.testing.expectEqual(user.instance_id, retained_identity.user_instance_id.?);
    var admitted = try credential.identity(alloc);
    defer admitted.deinit(alloc);
    try std.testing.expectEqual(user.instance_id, admitted.user_instance_id.?);
    try std.testing.expect(try http.tablePermissionCurrentlyAllowed(admitted, "docs", .read));
    var projected = (try http.cloneCatalogIdentity(alloc, admitted)).?;
    defer projected.deinit(alloc);
    try std.testing.expectEqual(user.instance_id, projected.user_instance_id.?);
    try http.projectCatalogIdentity(alloc, &projected, "docs", "private-table-17");
    const retained_policies = [_]OwnedRead.Policy{.{ .table = "private-table-17", .filter = null }};
    {
        var current = try credential.identity(alloc);
        defer current.deinit(alloc);
        try OwnedRead.validatePolicies(alloc, &current, projected, &retained_policies);
        // A row-filter change during execution invalidates buffered rows even
        // when table permission remains granted. Restoring it restores access.
        try manager.setRowFilter("alice", "docs", "{\"term\":{\"tenant\":\"changed\"}}");
        var after_wait = try credential.identity(alloc);
        defer after_wait.deinit(alloc);
        try std.testing.expectError(error.Forbidden, OwnedRead.validatePolicies(alloc, &after_wait, projected, &retained_policies));
        try manager.removeRowFilter("alice", "docs");
        var restored = try credential.identity(alloc);
        defer restored.deinit(alloc);
        try OwnedRead.validatePolicies(alloc, &restored, projected, &retained_policies);
        restored.user_instance_id = @splat(0);
        try std.testing.expectError(error.Forbidden, OwnedRead.validatePolicies(alloc, &restored, projected, &retained_policies));
        try manager.setRowFilter("alice", "*", "{\"term\":{\"tenant\":\"wildcard\"}}");
        try manager.setRowFilter("alice", "docs", "null");
        var override = try credential.identity(alloc);
        defer override.deinit(alloc);
        try std.testing.expect(http.effectiveRowFilterJson(override, "docs") == null);
        try http.projectCatalogIdentity(alloc, &override, "docs", "private-table-17");
        try std.testing.expect(http.effectiveRowFilterJson(override, "private-table-17") == null);
        try OwnedRead.validatePolicies(alloc, &override, projected, &retained_policies);
        try manager.removeRowFilter("alice", "*");
        try manager.removeRowFilter("alice", "docs");
    }
    const NativeCursor = struct {
        pages: usize = 0,
        columns: usize = 0,
        filters: usize = 0,
        counts: usize = 0,
        splits: usize = 0,
        closes: usize = 0,
        rotate_manager: ?*usermgr.UserManager = null,
        fn open(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
            return cursor(ptr);
        }
        fn cursor(ptr: *anyopaque) catalog.Cursor {
            return .{ .estimated_filtered_rows = 7, .estimated_rows = 99, .estimated_bytes = 1024, .ptr = ptr, .next = next, .next_columns = nextColumns, .count_rows = countRows, .set_dynamic_filter = setFilter, .split_scan = split, .split_ordered = split, .close = close };
        }
        fn next(ptr: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.pages += 1;
            return .{ .rows = &.{}, .after = "next" };
        }
        fn nextColumns(ptr: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.ColumnPage {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.columns += 1;
            return .{ .batch = .{ .snapshot = .{ .table_id = "docs", .snapshot_id = "v1" }, .row_refs = &.{}, .columns = &.{} }, .selection = &.{} };
        }
        fn countRows(ptr: *anyopaque) !?u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counts += 1;
            return 99;
        }
        fn setFilter(ptr: *anyopaque, _: *const @import("antfly_local_sources").sql_dynamic_filter.Filter) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.filters += 1;
            return true;
        }
        fn split(ptr: *anyopaque, a: std.mem.Allocator, _: usize) !?[]catalog.Cursor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const children = try a.alloc(catalog.Cursor, 2);
            @memset(children, cursor(ptr));
            self.splits += 1;
            return children;
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closes += 1;
        }
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.rotate_manager) |rotation_store| {
                self.rotate_manager = null;
                try rotation_store.updatePassword("alice", "rotated-during-checkpoint");
            }
        }
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
    {
        const saved = authority.request;
        defer authority.request = saved;
        const OffsetClock = struct {
            fn now(_: ?*anyopaque, clock: std.Io.Clock) std.Io.Timestamp {
                if (clock == .awake) return .{ .nanoseconds = std.time.ns_per_s };
                return std.testing.io.vtable.now(std.testing.io.userdata, clock);
            }
        };
        var vtable = std.testing.io.vtable.*;
        vtable.now = OffsetClock.now;
        authority.request.io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
        authority.request.deadline.raw.nanoseconds = 10 * std.time.ns_per_s;
        const before = @import("antfly_platform").time.monotonicNs();
        const normalized = try authority.context();
        const after = @import("antfly_platform").time.monotonicNs();
        try std.testing.expect(normalized.deadline_io == null);
        try std.testing.expect(normalized.deadline_ns.? >= before + 9 * std.time.ns_per_s);
        try std.testing.expect(normalized.deadline_ns.? <= after + 9 * std.time.ns_per_s);
        try normalized.ensureActive();
        authority.request.deadline.raw.nanoseconds = std.math.maxInt(i96);
        try std.testing.expect((try authority.context()).deadline_ns == null);
        authority.request.deadline.raw.nanoseconds = std.time.ns_per_s;
        try std.testing.expectError(error.QueryCanceled, authority.context());
        authority.request.deadline.raw.nanoseconds = 10 * std.time.ns_per_s;
        cancellation.store(true, .release);
        defer cancellation.store(false, .release);
        try std.testing.expectError(error.QueryCanceled, authority.context());
    }
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
    try std.testing.expectEqual(@as(?u64, 7), cursor.estimated_filtered_rows);
    try std.testing.expectEqual(@as(?u64, 99), cursor.estimated_rows);
    try std.testing.expectEqual(@as(?u64, 1024), cursor.estimated_bytes);
    _ = try cursor.next_columns.?(cursor.ptr, alloc, 1);
    try std.testing.expectEqual(@as(?u64, 99), try cursor.count_rows.?(cursor.ptr));
    const filter = try @import("antfly_local_sources").sql_dynamic_filter.Filter.create(alloc, &.{}, 128);
    defer filter.close();
    try std.testing.expect(try cursor.set_dynamic_filter.?(cursor.ptr, filter));
    const children = (try cursor.split_scan.?(cursor.ptr, alloc, 2)).?;
    defer alloc.free(children);
    var children_closed = false;
    defer if (!children_closed) for (children) |child| child.close(child.ptr);
    for (children) |child| {
        _ = try child.next_columns.?(child.ptr, alloc, 1);
        try std.testing.expectEqual(@as(?u64, 99), try child.count_rows.?(child.ptr));
    }
    const SplitSweep = struct {
        fn run(a: std.mem.Allocator, parent: catalog.Cursor, fixture: *NativeCursor, ordered: bool) !void {
            const before_splits = fixture.splits;
            const before_closes = fixture.closes;
            defer std.debug.assert(fixture.closes - before_closes == 2 * (fixture.splits - before_splits));
            const split = if (ordered) parent.split_ordered.? else parent.split_scan.?;
            const parts = (try split(parent.ptr, a, 2)).?;
            defer a.free(parts);
            defer for (parts) |part| part.close(part.ptr);
            for (parts) |part| try std.testing.expectEqual(@as(?u64, 99), part.estimated_rows);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, SplitSweep.run, .{ cursor, &native_cursor, false });
    try std.testing.checkAllAllocationFailures(alloc, SplitSweep.run, .{ cursor, &native_cursor, true });
    const closes_before_revoke = native_cursor.closes;
    try manager.removePermissionFromUser("alice", "docs", .table);
    {
        var current = try credential.identity(alloc);
        defer current.deinit(alloc);
        try std.testing.expectError(error.Forbidden, OwnedRead.validatePolicies(alloc, &current, projected, &retained_policies));
    }
    try std.testing.expectError(error.Forbidden, cursor.next(cursor.ptr, alloc, 1));
    try std.testing.expectError(error.Forbidden, cursor.next_columns.?(cursor.ptr, alloc, 1));
    try std.testing.expectError(error.Forbidden, cursor.count_rows.?(cursor.ptr));
    try std.testing.expectError(error.Forbidden, cursor.set_dynamic_filter.?(cursor.ptr, filter));
    try std.testing.expectError(error.Forbidden, cursor.split_scan.?(cursor.ptr, alloc, 2));
    try std.testing.expectError(error.Forbidden, cursor.split_ordered.?(cursor.ptr, alloc, 2));
    for (children) |child| {
        try std.testing.expectError(error.Forbidden, child.next_columns.?(child.ptr, alloc, 1));
        try std.testing.expectError(error.Forbidden, child.count_rows.?(child.ptr));
        child.close(child.ptr);
    }
    children_closed = true;
    try std.testing.expectEqual(@as(usize, 1), native_cursor.pages);
    try std.testing.expectEqual(@as(usize, 3), native_cursor.columns);
    try std.testing.expectEqual(@as(usize, 3), native_cursor.counts);
    try std.testing.expectEqual(@as(usize, 1), native_cursor.filters);
    cursor.close(cursor.ptr);
    cursor_closed = true;
    try std.testing.expectEqual(closes_before_revoke + 3, native_cursor.closes);
    try std.testing.expect(!(try http.tablePermissionCurrentlyAllowed(admitted, "docs", .read)));
    var fresh = try credential.identity(alloc);
    defer fresh.deinit(alloc);
    try std.testing.expect(!(try http.tablePermissionCurrentlyAllowed(fresh, "docs", .read)));
    try manager.updatePassword("alice", "new-secret");
    try std.testing.expectError(error.Unauthorized, credential.validate());
    try std.testing.expectError(error.Unauthorized, credential.identity(alloc));
    try std.testing.expectError(error.Unauthorized, retained_credential.validate());
    try std.testing.expectError(error.Unauthorized, retained_credential.identity(alloc));
    try std.testing.expectError(error.Unauthorized, retained_credential.cloneOwned(alloc));
    const replacement = try Credential.authenticate(alloc, &manager, "alice", "new-secret");
    defer Credential.release(replacement, alloc);
    var replacement_identity = try replacement.identity(alloc);
    defer replacement_identity.deinit(alloc);
    try std.testing.expect(!(try http.tablePermissionCurrentlyAllowed(replacement_identity, "docs", .read)));
    authority.credential = replacement;
    native_cursor.rotate_manager = &manager;
    try std.testing.expectError(error.Unauthorized, backend.vtable.checkpoint(backend.ptr));
    try std.testing.expect(native_cursor.rotate_manager == null);
    try std.testing.expectError(error.Unauthorized, replacement.validate());
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

test "SQL query waiting pgwire preserves deadline cancellation and typed parameters" {
    var cancel: std.atomic.Value(bool) = .init(false);
    const request: wire.Request = .{
        .statement = "select $1",
        .parameters = &.{ .{ .string = "value" }, .{ .integer = 42 } },
        .limit = 1,
        .io = std.testing.io,
        .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.Io.Clock.awake.now(std.testing.io).nanoseconds + std.time.ns_per_s } },
        .cancel_requested = &cancel,
    };
    const options = admissionOptions(request);
    try std.testing.expectEqual(@as(u64, @intCast(request.deadline.raw.nanoseconds)), options.deadline_ns.?);
    try std.testing.expect(options.retained_bytes >= request.statement.len + 5 + 2 * @sizeOf(std.json.Value));
    try options.check();
    cancel.store(true, .release);
    try std.testing.expectError(error.Canceled, options.check());
}

test "SQL pgwire retained input charge preserves page restrictions without traversing parameters" {
    var canceled: std.atomic.Value(bool) = .init(false);
    var request: wire.Request = .{
        // Neither field belongs to a new input capsule during cursor delivery.
        // Poison values make an accidental recount observable in this test.
        .statement = undefined,
        .parameters = undefined,
        .limit = 1,
        .io = std.testing.io,
        .deadline = .{ .clock = .awake, .raw = std.Io.Clock.awake.now(std.testing.io).addDuration(.fromSeconds(1)) },
        .cancel_requested = &canceled,
    };
    const options = admissionOptionsWithBytes(request, 12345);
    try std.testing.expectEqual(@as(u64, 12345), options.retained_bytes);
    try std.testing.expectEqual(@as(?u64, @intCast(request.deadline.raw.nanoseconds)), options.deadline_ns);
    try options.check();
    canceled.store(true, .release);
    try std.testing.expectError(error.Canceled, options.check());
    canceled.store(false, .release);
    request.deadline.raw.nanoseconds = 0;
    try std.testing.expectError(error.DeadlineExceeded, admissionOptionsWithBytes(request, 12345).check());
}

/// Normalize once at pgwire ingress. Native metadata/storage owners consume
/// platform monotonic deadlines; Io awake has a different epoch on macOS.
fn nativeRequestContext(request: *const wire.Request) !operation.RequestContext {
    try request.check();
    const context: operation.RequestContext = .{
        .deadline_ns = requestDeadlineNs(request.*),
        .deadline_io = io_abi.Borrow.init(&request.io),
        .fanout_io = io_abi.Borrow.init(&request.io),
        .cancellation = operation.CancellationToken.fromAtomic(request.cancel_requested),
    };
    return context.platformDeadline();
}

test "workload deadline normalizes pgwire clock before native metadata handoff" {
    var canceled = std.atomic.Value(bool).init(false);
    var request: wire.Request = .{ .statement = "SELECT 1", .limit = 1, .io = std.testing.io, .deadline = .{ .clock = .awake, .raw = std.Io.Clock.awake.now(std.testing.io).addDuration(.fromSeconds(1)) }, .cancel_requested = &canceled };
    const before = @import("antfly_platform").time.monotonicNs();
    const context = try nativeRequestContext(&request);
    const after = @import("antfly_platform").time.monotonicNs();
    try std.testing.expect(context.deadline_io == null);
    try std.testing.expect(context.fanout_io != null);
    try std.testing.expect(context.deadline_ns.? <= after + std.time.ns_per_s);
    try std.testing.expect(context.deadline_ns.? >= before);
    try context.ensureActive();
    request.deadline.raw.nanoseconds = std.math.maxInt(i96);
    const unlimited = try nativeRequestContext(&request);
    try std.testing.expect(unlimited.deadline_ns == null);
    try std.testing.expect(admissionOptions(request).deadline_ns == null);
    request.deadline.raw.nanoseconds = @as(i96, std.math.maxInt(u64)) + 1;
    const distant = try nativeRequestContext(&request);
    try std.testing.expect(distant.deadline_ns != null);
    try std.testing.expectEqual(@as(?u64, std.math.maxInt(u64)), admissionOptions(request).deadline_ns);
    canceled.store(true, .release);
    try std.testing.expectError(error.Canceled, context.ensureActive());
    try std.testing.expectError(error.QueryCanceled, nativeRequestContext(&request));
}

test "SQL pgwire materialized read pool retains result until wire release and rolls back allocation failure" {
    const Harness = struct {
        fn run(parent: std.mem.Allocator) !void {
            const memory = @import("../common/read_memory.zig");
            var pool = try memory.Pool.init(parent, null, .{ .bytes = @splat(1 << 20), .groups = @splat(1) });
            defer pool.deinit();
            const owner = try MaterializedRead.create(parent, &pool);
            var live = true;
            defer if (live) MaterializedRead.release(owner);
            owner.result = try @import("antfly_local_sources").sql_runtime.Result.singleText(owner.memory.allocator(), "SELECT", "n", "9007199254740993");
            try std.testing.expect(pool.stats().total_used > 0);
            try std.testing.expectEqual(@as(usize, 1), pool.stats().groups[0]);
            try std.testing.expectEqualStrings("9007199254740993", owner.result.?.output.rows[0][0].string);
            MaterializedRead.release(owner);
            live = false;
            try std.testing.expectEqual(@as(usize, 0), pool.stats().total_used);
            try std.testing.expectEqual(@as(usize, 0), pool.stats().groups[0]);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL pgwire retained lease releases after detached capsule physical cleanup" {
    const Budget = @import("../pgwire/budget.zig").Budget;
    const Fence = struct {
        connection: *Budget,
        service: *Budget,
        references: usize = 0,
        retired_before_release: bool = false,
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references += 1;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.retired_before_release = self.connection.used.load(.acquire) == 0 and self.service.used.load(.acquire) == 0;
            self.references -= 1;
        }
    };
    var connection: Budget = .{ .child = std.testing.allocator, .limit = 1 << 20 };
    var service: Budget = .{ .child = std.testing.allocator, .limit = 1 << 20 };
    var fence: Fence = .{ .connection = &connection, .service = &service };
    const alloc = connection.allocator();
    const service_alloc = service.allocator();
    var server: http.ApiHttpServer = undefined;
    server.alloc = service_alloc;
    var adapter: Adapter = .{ .server = &server };
    const credential = try service_alloc.create(Credential);
    credential.* = .{ .manager = undefined, .username = try service_alloc.dupe(u8, "alice"), .principal = try service_alloc.dupe(u8, "basic:alice"), .verifier_mac = @splat(0) };
    const owner = try alloc.create(OwnedRead);
    owner.* = undefined;
    owner.read_memory = null;
    owner.alloc = alloc;
    owner.adapter = &adapter;
    owner.cleanup = null;
    owner.cleanup_started = false;
    owner.physical_retired = false;
    owner.initRetirement(std.testing.io);
    owner.lifetime = .{ .context = owner, .retire = OwnedRead.retirePhysical, .destroy = OwnedRead.destroy, .detached = .init(true), .retirement_started = true };
    owner.arena = std.heap.ArenaAllocator.init(alloc);
    _ = try owner.arena.allocator().alloc(u8, 4096);
    owner.session_id = try service_alloc.dupe(u8, "retained-session");
    owner.identity = .{ .username = try service_alloc.dupe(u8, "alice"), .credential_principal = try service_alloc.dupe(u8, "basic:alice") };
    owner.authority = .{ .credential = credential, .identity = &owner.identity, .request = undefined };
    owner.retained_lease = (wire.RetainedLifetime{ .context = &fence, .retain_fn = Fence.retain, .release_fn = Fence.release }).retain();
    try std.testing.expect(connection.used.load(.acquire) > 0 and service.used.load(.acquire) > 0);
    OwnedRead.close(owner);
    try std.testing.expect(fence.retired_before_release);
    try std.testing.expectEqual(@as(usize, 0), fence.references);
}

test "SQL pgwire supervised cleanup retains native stream allocator root and parents through returned pages and task join" {
    const Budget = @import("../pgwire/budget.zig").Budget;
    const Fence = struct {
        connection: *Budget,
        service: *Budget,
        references: std.atomic.Value(usize) = .init(0),
        retired_before_release: std.atomic.Value(bool) = .init(false),
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.references.fetchAdd(1, .monotonic);
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.retired_before_release.store(self.connection.used.load(.acquire) == 0 and self.service.used.load(.acquire) == 0, .release);
            _ = self.references.fetchSub(1, .release);
        }
    };
    var cleanup_threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer cleanup_threads.deinit();
    var reaper_threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
    defer reaper_threads.deinit();
    var cleanup_memory: @import("../common/allocation_budget.zig").Shared = .{ .child = std.heap.smp_allocator, .limit = 1 << 16 };
    const cleanup_runtime = try @import("runnable_cleanup_owner.zig").Runtime.create(cleanup_memory.allocator(), reaper_threads.io(), 2);
    defer cleanup_runtime.deinit();
    var connection: Budget = .{ .child = std.testing.allocator, .limit = 1 << 20 };
    var service: Budget = .{ .child = std.testing.allocator, .limit = 1 << 20 };
    var fence: Fence = .{ .connection = &connection, .service = &service };
    const read_memory = @import("../common/read_memory.zig");
    var read_pool = try read_memory.Pool.init(std.testing.allocator, null, .{ .bytes = @splat(1 << 20), .groups = @splat(2) });
    defer read_pool.deinit();
    const alloc = connection.allocator();
    const service_alloc = service.allocator();
    var server: http.ApiHttpServer = undefined;
    server.alloc = service_alloc;
    server.cfg = .{};
    server.sql_plan_cache = @import("../sql/plan_cache.zig").Cache.init(std.testing.allocator, .{});
    const cache_io = server.sqlPlanCacheIo();
    defer server.sql_plan_cache.deinit(cache_io);
    var adapter: Adapter = .{ .server = &server };
    var scheduling = try http.RunnableScheduler.Scheduler.init(.{ .slots = 3, .max_operations = 4, .max_bytes = 128, .control_bytes = 32 });
    defer scheduling.close();
    var admission = http.RequestAdmission.init(1);
    const credential = try service_alloc.create(Credential);
    credential.* = .{ .manager = undefined, .username = try service_alloc.dupe(u8, "alice"), .principal = try service_alloc.dupe(u8, "basic:alice"), .verifier_mac = @splat(0) };
    const owner = try alloc.create(OwnedRead);
    owner.* = undefined;
    owner.read_memory = null;
    owner.alloc = alloc;
    owner.adapter = &adapter;
    owner.cleanup = null;
    owner.cleanup_started = false;
    owner.physical_retired = false;
    owner.initRetirement(std.testing.io);
    owner.lifetime = .{ .context = owner, .retire = OwnedRead.retirePhysical, .destroy = OwnedRead.destroy };
    owner.read_memory = try read_memory.Group.init(&read_pool, .general, 1 << 20);
    owner.arena = std.heap.ArenaAllocator.init(owner.readAllocator());
    _ = try owner.arena.allocator().alloc(u8, 1024);
    owner.session_id = try service_alloc.dupe(u8, "retained-session");
    owner.identity = .{ .username = try service_alloc.dupe(u8, "alice"), .credential_principal = try service_alloc.dupe(u8, "basic:alice") };
    owner.authority = .{ .credential = credential, .identity = &owner.identity, .request = undefined };
    owner.retained_lease = (wire.RetainedLifetime{ .context = &fence, .retain_fn = Fence.retain, .release_fn = Fence.release }).retain();
    owner.runnable = .{};
    try owner.runnable.start(&scheduling, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
    owner.native_adapter = .{ .server = &server, .identity = &owner.identity, .context = .{}, .runnable = &owner.runnable };
    owner.staged = .{};
    owner.range_guards = .{};
    owner.admission = admission.tryAcquireLease().?;
    var diagnostic: compiler.Diagnostic = .{};
    owner.plan = try server.sql_plan_cache.acquire(cache_io, .{ .statement = "SELECT 1" }, &diagnostic);
    // Open through production binding so cleanup owns a valid execution
    // context, typed parameter frame, cursor and regex allocator.
    const NativeFixture = struct {
        closed: std.atomic.Value(usize) = .init(0),
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = openScan, .mutate = mutate, .checkpoint = checkpoint } };
        }
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            return .{ .id = 1, .physical_name = "docs", .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .integer }} };
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.UnexpectedStatelessScan;
        }
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.UnexpectedMutation;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn openScan(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
            return .{ .ptr = raw, .next = next, .close = close };
        }
        fn next(_: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            return .{ .rows = &.{}, .after = null };
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.closed.fetchAdd(1, .release);
        }
    };
    var native_fixture: NativeFixture = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT n FROM docs WHERE n = $1", .{});
    defer compiled.deinit();
    owner.stream = (try Pull.Stream.open(owner.readAllocator(), native_fixture.backend(), &compiled, &.{.{ .integer = 7 }}, .{ .retained_bytes = 1 << 16 })) orelse return error.UnexpectedMaterializedFallback;
    owner.stream_live = true;
    try std.testing.expect(owner.stream.cursor != null);
    const invocation = owner.stream.parameter_invocation orelse return error.MissingParameterFrame;
    try std.testing.expectEqual(.integer, invocation.descriptors[0].kind.?);
    try std.testing.expectEqual(.int64, invocation.descriptors[0].element_type.?);
    try std.testing.expect(invocation.frame != null);
    var regex_lane = try owner.stream.regex_execution.acquire();
    regex_lane.deinit();
    var pages: [2]*OwnedRead.PageOwner = undefined;
    for (&pages) |*page| {
        page.* = try alloc.create(OwnedRead.PageOwner);
        var arena = std.heap.ArenaAllocator.init(owner.stream.budget.allocator());
        _ = try arena.allocator().alloc(u8, 4096);
        try owner.lifetime.retainResource();
        page.*.* = .{ .alloc = alloc, .read = owner, .page = .{ .arena = arena, .columns = &.{}, .values = .{ .rows = &.{} }, .exhausted = false } };
    }
    const cleanup_io = cleanup_threads.io();
    const cleanup_borrow = @import("antfly_runtime_abi").native_abi.IoBorrow.init(&cleanup_io);
    owner.cleanup = try cleanup_runtime.registry.reserve(cleanup_io, owner.native_adapter.providerRunnable().?, &.{ .io = &cleanup_borrow, .activation_budget = 1, .deadline_ns = 30 * std.time.ns_per_s }, owner, OwnedRead.cleanupCallbacks);
    try owner.cleanup.?.publish();
    try owner.runnable.park(.output_blocked);
    const pool_live = read_pool.stats();
    try std.testing.expect(pool_live.used[0] > 8192);
    try std.testing.expectEqual(@as(usize, 1), pool_live.groups[0]);
    OwnedRead.close(owner);
    try std.testing.expect(owner.lifetime.detached.load(.acquire));
    try std.testing.expectEqual(@as(usize, 3), owner.lifetime.references.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), fence.references.load(.acquire));
    try std.testing.expect(connection.used.load(.acquire) > 0 and service.used.load(.acquire) > 0);
    try std.testing.expectEqual(@as(usize, 1), admission.stats().in_flight);
    try std.testing.expectEqual(@as(usize, 1), server.sql_plan_cache.stats(cache_io).leases);
    try std.testing.expectEqual(@as(usize, 1), scheduling.stats().operations);
    try std.testing.expectEqual(@as(u64, 17), scheduling.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), scheduling.stats().runnable_slots);
    var control: http.RunnableScheduler.Execution = .{};
    try control.start(&scheduling, .{ .io = std.testing.io }, .control);
    control.finish();
    try std.testing.expectEqual(pool_live.total_used, read_pool.stats().total_used);
    OwnedRead.releasePage(pages[0]);
    try std.testing.expectEqual(@as(usize, 2), owner.lifetime.references.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), admission.stats().in_flight);
    try std.testing.expectEqual(@as(usize, 1), scheduling.stats().operations);
    try std.testing.expectEqual(@as(u64, 17), scheduling.stats().retained_bytes);
    // Last page deinit physically frees its arena and wrapper before stream
    // close, native plan/root retirement and final connection parent release.
    OwnedRead.releasePage(pages[1]);
    const deadline = std.Io.Clock.awake.now(std.testing.io).addDuration(.fromSeconds(5));
    while (cleanup_runtime.registry.retained() != 0) {
        if (std.Io.Clock.awake.now(std.testing.io).nanoseconds >= deadline.nanoseconds) return error.Timeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(usize, 1), native_fixture.closed.load(.acquire));
    try std.testing.expect(fence.retired_before_release.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), read_pool.stats().total_used);
    try std.testing.expectEqual(@as(usize, 0), read_pool.stats().groups[0]);
    try std.testing.expectEqual(@as(usize, 0), fence.references.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), connection.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), service.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), admission.stats().in_flight);
    try std.testing.expectEqual(@as(usize, 0), server.sql_plan_cache.stats(cache_io).leases);
    try std.testing.expectEqual(@as(usize, 0), scheduling.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduling.stats().retained_bytes);
}

test "SQL pgwire guarded cleanup checkpoint preserves independent deadline after foreground cancellation" {
    const runtime = @import("antfly_runtime_abi");
    const Clock = struct {
        var current: u64 = 0;
        fn now() u64 {
            return current;
        }
    };
    Clock.current = 0;
    var scheduling = try http.RunnableScheduler.Scheduler.init(.{ .slots = 3, .max_operations = 4, .max_bytes = 128, .control_bytes = 32 });
    defer scheduling.close();
    var owner: http.RunnableScheduler.Execution = .{};
    defer owner.finish();
    try owner.start(&scheduling, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 10, .retained_bytes = 17 }, .query);
    var server: http.ApiHttpServer = undefined;
    server.embedding_provider_runtime.io = std.testing.io;
    var identity: ?http.AuthenticatedIdentity = null;
    var canceled: std.atomic.Value(bool) = .init(false);
    var credential: Credential = undefined;
    const request: wire.Request = .{ .statement = "SELECT 1", .limit = 1, .io = std.testing.io, .deadline = .{ .clock = .awake, .raw = std.Io.Clock.awake.now(std.testing.io).addDuration(.fromSeconds(1)) }, .cancel_requested = &canceled };
    var authority: Authority = .{ .credential = &credential, .identity = &identity, .request = request };
    var native_adapter: execution.Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .runnable = &owner };
    var guarded: GuardedCatalog = .{ .native = native_adapter.backend(), .authority = &authority, .revision = &native_adapter.revision, .expected_guard = null, .cleanup = &owner };
    const borrow = native_adapter.providerRunnable().?;
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    const reserved = borrow.reserve_cleanup.?(borrow.context, &.{ .io = &io_borrow, .native_deadline = 1, .activation_budget = 1, .deadline_ns = 10 });
    if (!reserved.isOk()) return runtime.error_abi.errorFromStatus(reserved);
    canceled.store(true, .release);
    try std.testing.expectError(error.QueryCanceled, GuardedCatalog.checkpoint(&guarded));
    Clock.current = 1000;
    var cleanup_slots: usize = 0;
    const activated = borrow.activate_cleanup.?(borrow.context, &cleanup_slots);
    if (!activated.isOk()) return runtime.error_abi.errorFromStatus(activated);
    try GuardedCatalog.checkpoint(&guarded);
    Clock.current = 1010;
    try std.testing.expectError(error.DeadlineExceeded, GuardedCatalog.checkpoint(&guarded));
    try std.testing.expectEqual(@as(usize, 1), scheduling.stats().operations);
    try std.testing.expectEqual(@as(u64, 17), scheduling.stats().retained_bytes);
}

test "SQL pgwire controlled datetime normalization retires converted cells on cancellation" {
    const Cancellation = struct {
        checks: usize = 0,
        fn requested(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            return self.checks >= 16;
        }
        fn now() u64 {
            return 0;
        }
    };
    const input: [8]std.json.Value = @splat(.{ .integer = 1 });
    const types: [8]wire.Type = @splat(.datetime);
    var cancellation: Cancellation = .{};
    const options: @import("antfly_local_sources").common_bounded_admission.Options = .{ .io = std.testing.io, .retained_bytes = @sizeOf(@TypeOf(input)), .native_now_ns = Cancellation.now, .cancellation = .{ .ptr = &cancellation, .requested = Cancellation.requested } };
    var scheduler_owner = try http.RunnableScheduler.Scheduler.init(.{ .slots = 2, .checkpoint_budget = 64 });
    var runnable: http.RunnableScheduler.Execution = .{};
    defer runnable.finish();
    try runnable.start(&scheduler_owner, options, .query);
    cancellation.checks = 0;
    var owner: @import("sql_preparation.zig").Owner = .{ .execution = &runnable, .options = options };
    var budget: @import("antfly_local_sources").sql_memory_budget = .{ .backing = std.testing.allocator, .limit = 64 << 10 };
    try std.testing.expectError(error.Canceled, normalizeParametersControlled(budget.allocator(), &input, &types, &owner));
    try std.testing.expect(budget.peak > @sizeOf(@TypeOf(input)));
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expectEqual(@as(usize, 1), scheduler_owner.stats().operations);
    runnable.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler_owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler_owner.stats().retained_bytes);
}

test "SQL pgwire controlled datetime normalization retires all partial allocation failures" {
    const Harness = struct {
        fn run(allocator: std.mem.Allocator) !void {
            const options: @import("antfly_local_sources").common_bounded_admission.Options = .{ .io = std.testing.io };
            var runnable: http.RunnableScheduler.Execution = .{};
            defer runnable.finish();
            try runnable.start(null, options, .query);
            var owner: @import("sql_preparation.zig").Owner = .{ .execution = &runnable, .options = options };
            const values = try normalizeParametersControlled(allocator, &.{ .{ .integer = 1 }, .{ .integer = 2 }, .{ .integer = 3 } }, &.{ .datetime, .datetime, .datetime }, &owner);
            defer allocator.free(values);
            for (values) |value| allocator.free(value.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL pgwire controlled datetime normalization checks authority after final conversion" {
    const Probe = struct {
        allocations: usize = 0,
        cancel_at: ?usize = null,
        canceled: bool = false,
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const result = std.testing.allocator.rawAlloc(len, alignment, ra) orelse return null;
            self.allocations += 1;
            if (self.cancel_at == self.allocations) self.canceled = true;
            return result;
        }
        fn free(_: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            std.testing.allocator.rawFree(bytes, alignment, ra);
        }
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
        fn requested(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).canceled;
        }
        fn now() u64 {
            return 0;
        }
    };
    const input = [_]std.json.Value{ .{ .integer = 1 }, .{ .integer = 2 } };
    const types = [_]wire.Type{ .datetime, .datetime };
    var allocation_count: usize = 0;
    for ([_]bool{ false, true }) |interrupt| {
        var probe: Probe = .{ .cancel_at = if (interrupt) allocation_count else null };
        const options: @import("antfly_local_sources").common_bounded_admission.Options = .{
            .io = std.testing.io,
            .retained_bytes = @sizeOf(@TypeOf(input)),
            .native_now_ns = Probe.now,
            .cancellation = .{ .ptr = &probe, .requested = Probe.requested },
        };
        var scheduler_owner = try http.RunnableScheduler.Scheduler.init(.{ .slots = 2, .checkpoint_budget = 64 });
        var runnable: http.RunnableScheduler.Execution = .{};
        defer runnable.finish();
        try runnable.start(&scheduler_owner, options, .query);
        var owner: @import("sql_preparation.zig").Owner = .{ .execution = &runnable, .options = options };
        var budget: @import("antfly_local_sources").sql_memory_budget = .{ .backing = probe.allocator(), .limit = 64 << 10 };
        if (interrupt) {
            // Arm the last actual allocation measured with the same allocator
            // and inputs. Both converted cells exist before authority expires.
            try std.testing.expectError(error.Canceled, normalizeParametersControlled(budget.allocator(), &input, &types, &owner));
            try std.testing.expect(probe.canceled);
            try std.testing.expectEqual(allocation_count, probe.allocations);
        } else {
            const values = try normalizeParametersControlled(budget.allocator(), &input, &types, &owner);
            defer budget.allocator().free(values);
            for (values) |value| budget.allocator().free(value.string);
            allocation_count = probe.allocations;
            try std.testing.expect(allocation_count >= 3);
        }
        try std.testing.expect(budget.peak > @sizeOf(@TypeOf(input)));
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        try std.testing.expectEqual(@as(usize, 1), scheduler_owner.stats().operations);
        try std.testing.expectEqual(@as(u64, @sizeOf(@TypeOf(input))), scheduler_owner.stats().retained_bytes);
        runnable.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler_owner.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), scheduler_owner.stats().runnable_slots);
        try std.testing.expectEqual(@as(u64, 0), scheduler_owner.stats().retained_bytes);
    }
}

test "SQL pgwire supervised cleanup releases foreground admission while remote uncertainty remains node owned" {
    const retirement = @import("retained_read_retirement.zig");
    const Budget = @import("../pgwire/budget.zig").Budget;
    const Parents = struct {
        connection: *Budget,
        service: *Budget,
        references: std.atomic.Value(usize) = .init(0),
        released: std.Io.Event = .unset,
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.references.fetchAdd(1, .acq_rel);
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.connection.used.load(.acquire) == 0 and self.service.used.load(.acquire) == 0);
            std.debug.assert(self.references.fetchSub(1, .acq_rel) == 1);
            self.released.set(std.testing.io);
        }
    };
    const Node = struct {
        registry: *retirement.Registry,
        pins: std.atomic.Value(usize) = .init(0),
        pub fn bind(self: *@This(), parent: retirement.Parent) !retirement.Binding {
            var owned = parent;
            owned.pin = .{ .ptr = self, .retain = retain, .release = release };
            return .{ .registry = self.registry, .parent = owned };
        }
        fn retain(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.pins.fetchAdd(1, .acq_rel);
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.pins.fetchSub(1, .acq_rel) != 0);
        }
    };
    const Source = struct {
        owner: *OwnedRead,
        node: *Node,
        ledger: *retirement.Registry,
        fail_open: bool,
        capture_on_resolve: bool = false,
        acknowledged: bool = false,
        published: usize = 0,
        destroyed: usize = 0,
        closed: std.atomic.Value(usize) = .init(0),
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = openScan, .mutate = mutate, .checkpoint = checkpoint } };
        }
        fn resolve(raw: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.owner.native_adapter.shared_receipt_wait_budget.?.deadline(1, 1);
            if (self.capture_on_resolve and self.published == 0) try self.publish();
            return .{ .id = 1, .physical_name = "docs", .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .integer }} };
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.UnexpectedStatelessScan;
        }
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.UnexpectedMutation;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn openScan(raw: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.publish();
            if (self.fail_open) return error.TestPublishedCaptureFailure;
            return .{ .ptr = raw, .next = next, .close = close };
        }
        fn publish(self: *@This()) !void {
            // Model an ambiguously published remote capture while exercising
            // the real SQL open, prepublished supervisor and parent callbacks.
            const binding = self.owner.native_adapter.read_retirement orelse return error.TestMissingReadBinding;
            std.debug.assert(binding.registry == self.ledger);
            const ticket = try binding.registry.reserve(binding.parent, 64);
            try ticket.bind(.{ .ptr = self, .poll = poll, .destroy = destroy });
            try ticket.publish();
            self.published += 1;
            try ticket.transfer();
        }
        fn next(_: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            return .{ .rows = &.{}, .after = null };
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.closed.fetchAdd(1, .acq_rel);
        }
        fn poll(raw: *anyopaque, _: retirement.Poll) !retirement.Outcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (self.acknowledged) .retired else .pending;
        }
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.node.pins.load(.acquire) != 0);
            self.destroyed += 1;
        }
    };
    const Mode = enum { failed_capture, late_error, declined };
    for ([_]Mode{ .failed_capture, .late_error, .declined }) |mode| for ([_]bool{ true, false }) |early_ack| {
        const failed_setup = mode == .failed_capture;
        const has_remote = mode != .declined or !early_ack;
        var cleanup_threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
        defer cleanup_threads.deinit();
        var reaper_threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
        defer reaper_threads.deinit();
        const supervisor = try @import("runnable_cleanup_owner.zig").Runtime.create(std.testing.allocator, reaper_threads.io(), 1);
        defer supervisor.deinit();
        var ledger = try retirement.Registry.init(std.testing.allocator, 1, 4096, 4096);
        defer {
            ledger.close();
            ledger.deinit() catch unreachable;
        }
        var connection: Budget = .{ .child = std.testing.allocator, .limit = 1 << 20 };
        var service: Budget = .{ .child = std.testing.allocator, .limit = 1 << 20 };
        var parents: Parents = .{ .connection = &connection, .service = &service };
        var server: http.ApiHttpServer = undefined;
        server.alloc = service.allocator();
        server.cfg = .{};
        server.sql_plan_cache = @import("../sql/plan_cache.zig").Cache.init(std.testing.allocator, .{});
        const cache_io = server.sqlPlanCacheIo();
        defer server.sql_plan_cache.deinit(cache_io);
        var adapter: Adapter = .{ .server = &server };
        var scheduling = try http.RunnableScheduler.Scheduler.init(.{ .slots = 2, .max_operations = 4, .max_bytes = 4 << 20, .control_bytes = 1 << 20 });
        defer scheduling.close();
        var admission = http.RequestAdmission.init(1);
        const alloc = connection.allocator();
        const credential = try server.alloc.create(Credential);
        credential.* = .{ .manager = undefined, .username = try server.alloc.dupe(u8, "alice"), .principal = try server.alloc.dupe(u8, "basic:alice"), .verifier_mac = @splat(0) };
        const owner = try alloc.create(OwnedRead);
        owner.* = undefined;
        owner.alloc = alloc;
        owner.adapter = &adapter;
        owner.read_memory = null;
        owner.cleanup = null;
        owner.cleanup_started = false;
        owner.physical_retired = false;
        owner.initRetirement(std.testing.io);
        owner.lifetime = .{ .context = owner, .retire = OwnedRead.retirePhysical, .destroy = OwnedRead.destroy };
        owner.arena = std.heap.ArenaAllocator.init(alloc);
        _ = try owner.arena.allocator().alloc(u8, 1024);
        owner.session_id = null;
        owner.identity = .{ .username = try server.alloc.dupe(u8, "alice"), .credential_principal = try server.alloc.dupe(u8, "basic:alice") };
        owner.authority = .{ .credential = credential, .identity = &owner.identity, .request = undefined };
        owner.runnable = .{};
        try owner.runnable.start(&scheduling, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
        var receipt_budget: @import("antfly_local_sources").sql_read_requirements.WaitBudget = .{};
        owner.native_adapter = .{ .server = &server, .identity = &owner.identity, .context = .{}, .runnable = &owner.runnable, .shared_receipt_wait_budget = &receipt_budget };
        owner.staged = .{};
        owner.range_guards = .{};
        var diagnostic: compiler.Diagnostic = .{};
        const plan = try server.sql_plan_cache.acquire(cache_io, .{ .statement = if (mode == .declined) "SELECT COUNT(*) FROM docs" else "SELECT n FROM docs WHERE n = $1" }, &diagnostic);
        var retained = (wire.RetainedLifetime{ .context = &parents, .retain_fn = Parents.retain, .release_fn = Parents.release }).retain();
        const cleanup_io = cleanup_threads.io();
        const io_borrow = @import("antfly_runtime_abi").native_abi.IoBorrow.init(&cleanup_io);
        owner.cleanup = try supervisor.registry.reserve(cleanup_io, owner.native_adapter.providerRunnable().?, &.{ .io = &io_borrow, .activation_budget = 1, .deadline_ns = 30 * std.time.ns_per_s }, owner, OwnedRead.cleanupCallbacks);
        owner.publishSetup(plan, admission.tryAcquireLease().?, &retained);
        try std.testing.expect(retained.lifetime == null);
        try std.testing.expect(owner.authority.request.retained_lifetime == null);
        const Transport = struct {
            fn unused(_: *anyopaque, _: std.mem.Allocator, _: @import("antfly_local_sources").common_http_http_common.HttpRequest) !@import("antfly_local_sources").common_http_http_common.HttpResponse {
                return error.UnexpectedTestTransport;
            }
        };
        const transport: @import("antfly_local_sources").common_http_http_common.RequestExecutor = .{ .ptr = &parents, .clock_io = .init(&std.testing.io), .vtable = &.{ .execute = Transport.unused } };
        const protocol: @import("remote_attempt_client.zig").Client = .{ .ledger = undefined, .store = undefined, .targets = &.{}, .signing = .{ .secret = "test-secret-unused-32-bytes-long-value", .issuer = "cluster" }, .read_allocator = server.alloc, .control_allocator = server.alloc, .read_executor = transport, .control_executor = transport };
        var node: Node = .{ .registry = &ledger };
        const remote = try @import("remote_read_operation_owner.zig").Owner.create(std.testing.allocator, &scheduling, &node);
        owner.remote_owner = remote;
        var selected_protocol = protocol;
        selected_protocol.reads_ready = early_ack;
        // Local capture/setup works with either a ready or rejecting remote
        // descriptor. This fixture publishes simulated retirement obligations;
        // the native provisioned recovery fixture checks actual remote refusal.
        try owner.bindRemoteReads(selected_protocol, remote);
        try std.testing.expectEqual(early_ack, owner.remote_dispatch.?.client.reads_ready);
        try std.testing.expect(owner.native_adapter.read_remote_executor.?.ptr == @as(*anyopaque, &owner.remote_dispatch.?));
        try std.testing.expectError(error.InvalidRunnableTransition, owner.bindRemoteReads(protocol, remote));
        try owner.native_adapter.beginReadFence();
        var source: Source = .{ .owner = owner, .node = &node, .ledger = &ledger, .fail_open = failed_setup, .capture_on_resolve = mode == .declined and has_remote };
        if (mode == .declined) {
            try std.testing.expect(!try owner.captureStream(source.backend(), plan.compiled(), &.{}, .{ .retained_bytes = 1 << 16 }));
        } else if (failed_setup) {
            try std.testing.expectError(error.TestPublishedCaptureFailure, owner.captureStream(source.backend(), plan.compiled(), &.{.{ .integer = 7 }}, .{ .retained_bytes = 1 << 16 }));
            try std.testing.expect(!owner.stream_live);
        } else try std.testing.expect(try owner.captureStream(source.backend(), plan.compiled(), &.{.{ .integer = 7 }}, .{ .retained_bytes = 1 << 16 }));
        try std.testing.expectEqual(@as(usize, if (has_remote) 1 else 0), source.published);
        const charged = scheduling.stats().retained_bytes;
        const remote_baseline: u64 = @sizeOf(@import("remote_read_operation_owner.zig").Owner);
        try std.testing.expectEqual(remote_baseline, scheduling.control_baseline_bytes);
        try std.testing.expectEqual(17 + remote_baseline, charged);
        try std.testing.expectEqual(if (has_remote) retirement.Registry.Progress.pending else .idle, ledger.pollOne(.{ .allocator = std.testing.allocator }));
        if (early_ack and has_remote) {
            source.acknowledged = true;
            try std.testing.expectEqual(retirement.Registry.Progress.retired, ledger.pollOne(.{ .allocator = std.testing.allocator }));
            try std.testing.expectEqual(@as(usize, 1), scheduling.stats().operations);
            try std.testing.expectEqual(charged, scheduling.stats().retained_bytes);
        }
        if (mode != .declined) try owner.runnable.rejectRetained(error.Canceled);
        // Same retirement path as open()'s post-publication errdefer, including
        // an already opened native stream followed by a late setup failure.
        try std.testing.expectEqual(@as(u64, 1 + std.time.ns_per_ms), receipt_budget.deadline_ns.?);
        owner.sealSetup();
        try std.testing.expect(owner.native_adapter.shared_receipt_wait_budget == null);
        try std.testing.expect(owner.authority.request.read_receipt_budget == null);
        receipt_budget = .{};
        try std.testing.expectEqual(@as(u64, 1 + std.time.ns_per_ms), owner.native_adapter.receipt_wait_budget.deadline_ns.?);
        if (mode == .declined) {
            var canceled: std.atomic.Value(bool) = .init(false);
            const request: wire.Request = .{ .io = std.testing.io, .deadline = std.Io.Clock.Timestamp.now(std.testing.io, .awake).addDuration(.{ .raw = .fromSeconds(5), .clock = .awake }), .statement = "SELECT COUNT(*) FROM docs", .limit = 1, .cancel_requested = &canceled };
            var setup_owned = true;
            try owner.retireForFallback(&setup_owned, request);
            // Foreground cleanup releases admission even when the peer's
            // terminal response has not yet arrived.
            var lease = admission.tryAcquireLease() orelse return error.TestFallbackRetainedAdmission;
            lease.release();
            try std.testing.expect(!setup_owned);
        } else owner.retireSetup();
        const deadline = std.Io.Clock.awake.now(std.testing.io).addDuration(.fromSeconds(5));
        while (supervisor.registry.retained() != 0) {
            if (std.Io.Clock.awake.now(std.testing.io).nanoseconds >= deadline.nanoseconds) return error.Timeout;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expectEqual(@as(usize, if (mode == .late_error) 1 else 0), source.closed.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), scheduling.stats().runnable_slots);
        if (!early_ack and has_remote) {
            // The old owner is destroyed. No deferred callback may inspect it.
            try std.testing.expect(parents.released.isSet());
            try std.testing.expectEqual(@as(usize, 0), scheduling.stats().operations);
            // Only independently reserved node metadata remains charged.
            try std.testing.expectEqual(remote_baseline, scheduling.control_baseline_bytes);
            try std.testing.expectEqual(remote_baseline, scheduling.stats().retained_bytes);
            try std.testing.expectEqual(@as(usize, 0), admission.stats().in_flight);
            try std.testing.expectEqual(@as(usize, 0), parents.references.load(.acquire));
            try std.testing.expectEqual(@as(usize, 0), connection.used.load(.acquire));
            try std.testing.expectEqual(@as(usize, 0), service.used.load(.acquire));
            try std.testing.expect(node.pins.load(.acquire) != 0);
            try std.testing.expectEqual(@as(usize, 1), ledger.snapshot().retained);
            var local_query = admission.tryAcquireLease() orelse return error.TestUncertaintyRetainedAdmission;
            local_query.release();
            try std.testing.expectEqual(@as(usize, 0), server.sql_plan_cache.stats(cache_io).leases);
            var health: http.RunnableScheduler.Execution = .{};
            try health.start(&scheduling, .{ .io = std.testing.io, .allow_wait = false }, .control);
            health.finish();
            source.acknowledged = true;
            try std.testing.expectEqual(retirement.Registry.Progress.retired, ledger.pollOne(.{ .allocator = std.testing.allocator }));
        }
        try std.testing.expectEqual(@as(usize, if (has_remote) 1 else 0), source.destroyed);
        try std.testing.expectEqual(@as(usize, 0), node.pins.load(.acquire));
        try std.testing.expectEqual(@as(u64, 0), scheduling.control_baseline_bytes);
        try std.testing.expect(parents.released.isSet());
        try std.testing.expectEqual(@as(usize, 0), scheduling.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduling.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 0), admission.stats().in_flight);
        try std.testing.expectEqual(@as(usize, 0), connection.used.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), service.used.load(.acquire));
    };
}

test "SQL pgwire native frozen JSON renders exact numbers without scalar delivery" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            const sources = @import("antfly_local_sources");
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const alloc = arena.allocator();
            for ([_]bool{ false, true }) |parse_numbers| {
                const root = try std.json.parseFromSliceLeaky(std.json.Value, alloc, "{\"x\":[9007199254740993,1.25],\"s\":\"a\\nb\"}", .{ .parse_numbers = parse_numbers });
                const rows = [_]sources.sql_operators.Row{.{ .values = &.{sources.sql_scalar.Datum.json(root)}, .keys = &.{}, .ordinal = 0 }};
                for ([_]sources.sql_spill.ColumnarVersion{ .v1, .v2 }) |version| {
                    const bytes = try sources.sql_spill.encodeColumnarBlockVersionAlloc(alloc, &rows, 1 << 20, version);
                    const block = try sources.sql_spill.decodeColumnarBlockWithCache(alloc, a, bytes, 1 << 20);
                    defer block.deinit();
                    const Reader = struct {
                        value: sources.sql_execution_batch.FrozenJson,
                        fn cell(_: *anyopaque, _: std.mem.Allocator, _: usize, _: usize) !sources.sql_scalar.Datum {
                            return error.UnexpectedScalarMaterialization;
                        }
                        fn frozen(raw: *anyopaque, _: usize, _: usize) !?sources.sql_execution_batch.FrozenJson {
                            return @as(*@This(), @ptrCast(@alignCast(raw))).value;
                        }
                    };
                    var reader: Reader = .{ .value = (try block.frozenJson(0, 0, false)).? };
                    var page: Pull.BatchPage = .{
                        .arena = .init(a),
                        .columns = &.{.{ .name = "j", .type = .json }},
                        .values = .{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_frozen = Reader.frozen, .count = 1, .width = 1 } },
                        .exhausted = true,
                    };
                    defer page.deinit();
                    var scratch = std.heap.FixedBufferAllocator.init(&.{});
                    const cell = try OwnedRead.PageOwner.pageCell(&page, scratch.allocator(), 0, 0);
                    try std.testing.expect(!cell.sql_null and cell.value == .null and cell.json_bytes == null and cell.frozen_json != null);
                    const expected = try std.json.Stringify.valueAlloc(alloc, root, .{});
                    for ([_]u16{ 0, 1 }) |format| {
                        var buffer: [256]u8 = undefined;
                        var output: std.Io.Writer = .fixed(&buffer);
                        try wire_values.encodeCellInto(scratch.allocator(), &output, .{ .name = "j", .type = .json }, format, cell, buffer.len);
                        const prefix: usize = @intFromBool(format == 1);
                        if (prefix != 0) try std.testing.expectEqual(@as(u8, 1), output.buffered()[0]);
                        try std.testing.expectEqualStrings(expected, output.buffered()[prefix..]);
                    }
                }
            }
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL pgwire native consumer checkpoints its own runnable while producer remains parked" {
    const Runnable = http.RunnableScheduler;
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 2, .checkpoint_budget = 1 });
    var producer: Runnable.Execution = .{};
    try producer.start(&scheduler, .{ .io = std.testing.io }, .query);
    defer producer.finish();
    try producer.park(.output_blocked);
    const Fixture = struct {
        producer: *Runnable.Execution,
        calls: usize = 0,
        fn consume(raw: *anyopaque, control: ?wire.ReadControl) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const owner = control orelse return error.MissingConsumer;
            for (0..8) |_| {
                try std.testing.expectEqual(Runnable.State.output_blocked, self.producer.entry.?.state);
                try owner.checkpoint(owner.ptr);
                self.calls += 1;
            }
        }
    };
    var fixture: Fixture = .{ .producer = &producer };
    var cancel: std.atomic.Value(bool) = .init(false);
    var job: DeliveryJob = .{ .adapter = undefined, .credential = undefined, .request = .{ .statement = "SELECT body", .limit = 1, .io = std.testing.io, .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i64) } }, .cancel_requested = &cancel }, .work = .{ .context = &fixture, .run = Fixture.consume } };
    try job.execution.start(&scheduler, .{ .io = std.testing.io }, .query);
    defer job.execution.finish();
    try job.consume();
    try std.testing.expectEqual(@as(usize, 8), fixture.calls);
    cancel.store(true, .release);
    try std.testing.expectError(error.QueryCanceled, job.consume());
    try std.testing.expectEqual(@as(usize, 8), fixture.calls);
    try std.testing.expectEqual(Runnable.State.output_blocked, producer.entry.?.state);
}

test "SQL pgwire native consumer shared worker progresses while the only read admission is retained" {
    const a = std.testing.allocator;
    const casbin = @import("antfly_casbin");
    var store = usermgr.MemoryStore.init(a);
    defer store.deinit();
    var policies = casbin.MemoryAdapter.init(a);
    defer policies.deinit();
    var manager = try usermgr.UserManager.init(a, store.iface(), try usermgr.initDefaultEnforcer(a, policies.iface()));
    defer manager.deinit();
    var user = try manager.createUser("delivery", "secret", &.{});
    defer user.deinit(a);
    const credential = try Credential.authenticate(a, &manager, "delivery", "secret");
    defer Credential.release(credential, a);
    var runtime = try @import("../storage/helper_runtime_resources.zig").Owner.init(a, .{ .limit_bytes = 4 << 30 });
    defer runtime.deinit();
    const Status = struct {
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };
    var raw: u8 = 0;
    var server = try http.ApiHttpServer.initWithConfig(a, .{
        .backend_runtime = runtime.ptr(),
        .user_manager = &manager,
        .query_max_concurrent_requests = 1,
        .request_memory_bytes = 8 << 20,
        .retained_memory_bytes = 8 << 20,
        .runnable_policy = .{ .slots = 2, .checkpoint_budget = 1, .max_wait_ms = 1000 },
    }, .{ .ptr = &raw, .vtable = &.{ .status = Status.status } }, null, null);
    defer server.deinit();
    var read = try server.acquireSqlExecution(false);
    defer read.release();
    const Fixture = struct {
        server: *http.ApiHttpServer,
        caller: std.Thread.Id,
        calls: usize = 0,
        fn consume(raw_: *anyopaque, control: ?wire.ReadControl) !void {
            const self: *@This() = @ptrCast(@alignCast(raw_));
            try std.testing.expect(std.Thread.getCurrentId() != self.caller);
            try std.testing.expectError(error.SqlWriteCapacityUnavailable, self.server.acquireSqlExecution(false));
            const owner = control orelse return error.MissingConsumer;
            const job: *DeliveryJob = @ptrCast(@alignCast(owner.ptr.?));
            try std.testing.expectEqual(http.RunnableScheduler.State.running, job.execution.entry.?.state);
            for (0..8) |_| {
                try owner.checkpoint(owner.ptr);
                self.calls += 1;
            }
        }
    };
    var fixture: Fixture = .{ .server = &server, .caller = std.Thread.getCurrentId() };
    var adapter: Adapter = .{ .server = &server };
    var canceled: std.atomic.Value(bool) = .init(false);
    const request: wire.Request = .{ .statement = "SELECT body", .limit = 1, .io = std.testing.io, .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } }, .cancel_requested = &canceled };
    const identity: wire.Identity = .{ .context = credential, .release = Credential.release };
    try Adapter.deliverRow(&adapter, identity, request, .{ .context = &fixture, .run = Fixture.consume });
    try std.testing.expectEqual(@as(usize, 8), fixture.calls);
    try std.testing.expectError(error.SqlWriteCapacityUnavailable, server.acquireSqlExecution(false));
    canceled.store(true, .release);
    try std.testing.expectError(error.QueryCanceled, Adapter.deliverRow(&adapter, identity, request, .{ .context = &fixture, .run = Fixture.consume }));
    try std.testing.expectEqual(@as(usize, 8), fixture.calls);
}
