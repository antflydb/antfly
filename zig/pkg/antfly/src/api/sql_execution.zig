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

//! SQL adapter to the existing authenticated catalog and relational row APIs.
const std = @import("std");
const http_server = @import("http_server.zig");
const catalog = @import("antfly_local_sources").sql_catalog;
const ast = @import("antfly_local_sources").sql_ast;
const system_catalog = @import("antfly_local_sources").system_catalog_domain;
const operation = @import("antfly_local_sources").api_operation;
const helpers = @import("http_route_helpers.zig");
const wire = @import("antfly_metadata_openapi").types;
const db_types = @import("antfly_local_sources").storage_db_types;
const setting_catalog = @import("antfly_local_sources").sql_setting_catalog;
const read_fence = @import("antfly_local_sources").sql_read_fence;

const SettingOverlaySource = enum { durable_session, connection };

fn attachedSettingOverlay(source: SettingOverlaySource, caller: []const setting_catalog.OverlayEntry, durable: []const setting_catalog.OverlayEntry) ![]const setting_catalog.OverlayEntry {
    return switch (source) {
        .durable_session => durable,
        .connection => if (durable.len == 0) caller else error.SettingOverlayConflict,
    };
}

fn refreshAttachedSettingOverlay(
    registry: *@import("transactions.zig").SessionRegistry,
    alloc: std.mem.Allocator,
    id: db_types.TxnId,
    source: SettingOverlaySource,
    caller: []const setting_catalog.OverlayEntry,
    retained: *?@import("transactions.zig").SessionRegistry.SqlState,
) ![]const setting_catalog.OverlayEntry {
    retained.* = (try registry.getSqlState(alloc, id)) orelse return error.SqlTransactionNotActive;
    return attachedSettingOverlay(source, caller, retained.*.?.setting_active.items);
}

test "SQL attached setting overlay keeps HTTP durable authority separate from pgwire connection state" {
    const caller = [_]setting_catalog.OverlayEntry{.{ .identity = .{ .id = 1, .generation = 1 }, .value = .{ .integer = 99 } }};
    const durable = [_]setting_catalog.OverlayEntry{.{ .identity = .{ .id = 1, .generation = 1 }, .value = .{ .integer = 5 } }};
    const http = try attachedSettingOverlay(.durable_session, &caller, &durable);
    try std.testing.expectEqual(@as(i64, 5), http[0].value.integer);
    const pgwire = try attachedSettingOverlay(.connection, &caller, &.{});
    try std.testing.expectEqual(@as(i64, 99), pgwire[0].value.integer);
    try std.testing.expectError(error.SettingOverlayConflict, attachedSettingOverlay(.connection, &caller, &durable));
}

test "SQL attached setting overlay refresh observes a setting committed after attachment" {
    const alloc = std.testing.allocator;
    var registry = @import("transactions.zig").SessionRegistry.init(null);
    defer registry.deinit(alloc);
    const Fixture = struct {
        const definitions = [_]setting_catalog.Definition{.{
            .identity = .{ .id = 1, .generation = 1 },
            .name = "app.limit",
            .kind = .integer,
            .session_writable = true,
            .default = .{ .integer = 3 },
        }};
        fn load(_: *anyopaque, _: std.mem.Allocator, scope: setting_catalog.Scope) !setting_catalog.RawSnapshot {
            return .{ .scope = scope, .epoch = 1, .definitions = &definitions };
        }
    };
    var marker: u8 = 0;
    const owner: setting_catalog.Owner = .{ .ptr = &marker, .load = Fixture.load };
    const begun = try registry.beginForPrincipal(alloc, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7, "alice");
    try registry.setSqlSetting(alloc, begun.txn_id, "alice", owner, "app.limit", .{ .integer = 5 }, false);
    // Model an attachment that precedes another request's committed SET;
    // the lease-held read must not reuse this earlier owned snapshot.
    var attached = (try registry.getSqlState(alloc, begun.txn_id)).?;
    defer attached.deinit(alloc);
    {
        const mutation_lease = registry.tryAcquireCommitExecution(begun.txn_id) orelse return error.TestExpectedSessionLease;
        defer mutation_lease.release();
        try registry.setSqlSetting(alloc, begun.txn_id, "alice", owner, "app.limit", .{ .integer = 7 }, false);
    }
    const lease = registry.tryAcquireCommitExecution(begun.txn_id) orelse return error.TestExpectedSessionLease;
    defer lease.release();
    var refreshed: ?@import("transactions.zig").SessionRegistry.SqlState = null;
    defer if (refreshed) |*state| state.deinit(alloc);
    const overlay = try refreshAttachedSettingOverlay(&registry, alloc, begun.txn_id, .durable_session, attached.setting_active.items, &refreshed);
    try std.testing.expectEqual(@as(i64, 5), attached.setting_active.items[0].value.integer);
    try std.testing.expectEqual(@as(i64, 7), overlay[0].value.integer);
}

fn supportsRangeGuards(server: *const http_server.ApiHttpServer) bool {
    const reads = server.table_reads orelse return false;
    const writes = server.table_writes orelse return false;
    return reads.supports_sql_range_guards and writes.supports_sql_range_guards and
        reads.vtable.open_relational_statement != null and
        (writes.vtable.commit_transaction_with_id != null or writes.vtable.commit_transaction_with_id_with_cancellation != null);
}

fn rejectTransactionalDdl(statement: ast.Statement) !void {
    // Catalog and publication operations have their own durable commit
    // boundary; a surrounding SQL ROLLBACK cannot undo them.
    if (@import("antfly_local_sources").sql_ddl_runtime.accepts(statement)) return error.UnsupportedSqlExecution;
}

const ReceiptWaitBudget = @import("antfly_local_sources").sql_read_requirements.WaitBudget;

/// Synchronous setup borrow from the local query capsule. It never becomes
/// a remote retention parent; setup publishes only independent node owners.
pub const RemoteReadSetup = struct {
    ptr: *anyopaque,
    prepare: *const fn (*anyopaque) anyerror!void,
};

pub const Adapter = struct {
    lake_visibility: enum { committed, accepted } = .committed,
    read_requirements: @import("antfly_local_sources").sql_read_requirements.Requirements = .{},
    receipt_wait_budget: ReceiptWaitBudget = .{},
    shared_receipt_wait_budget: ?*ReceiptWaitBudget = null,
    /// A transport may consume borrowed result columns within the same policy,
    /// settings and snapshot scope as ordinary execution. Mutation and active
    /// transaction paths continue to own their acknowledgement result.
    read_delivery: ?struct {
        ptr: *anyopaque,
        deliver: *const fn (*anyopaque, std.mem.Allocator, *@import("antfly_local_sources").sql_read_stream.Stream) anyerror!@import("antfly_local_sources").sql_runtime.Result,
    } = null,
    server: *http_server.ApiHttpServer,
    identity: *?http_server.AuthenticatedIdentity,
    context: operation.RequestContext,
    runnable: ?*http_server.RunnableScheduler.Execution = null,
    read_remote_setup: ?RemoteReadSetup = null,
    read_retirement: ?@import("retained_read_retirement.zig").Binding = null,
    read_remote_executor: ?@import("antfly_local_sources").common_http_http_common.RequestExecutor = null,
    /// Stable receivers for preauthorized physical read cleanup, retained by
    /// this Adapter through every child join and the owning execution finish.
    cleanup_phase: ?@import("runnable_local_cleanup.zig").Cleanup = null,
    /// Write teams retain their grant through accepted durable completion;
    /// they do not borrow read-only suspension hooks.
    write_execution: ?*http_server.RunnableScheduler.Execution = null,
    /// Ingress-owned stable receivers survive through the root owning finish.
    write_completion: ?*@import("runnable_write_team.zig").Completion = null,
    /// SQL preparation remains serial. Participant fanout separately reserves
    /// its bounded team on the same retained execution.
    serial_execution: bool = false,
    read_guard: ?read_fence.Guard = null,
    /// Preauthorized foreground storage for policy validation across CPU-stage
    /// handoff. It outlives every policy callback and is retired with the fence.
    read_policy_memory: ?@import("read_policy_memory.zig").Reservation = null,
    /// The synchronous HTTP worker keeps this owned guard through response
    /// serialization. That caller must end the fence before retiring its root.
    retain_read_guard: bool = false,
    database: []const u8 = "default",
    namespace: []const u8 = "public",
    /// Borrowed from the pinned connection request, never retained by a plan.
    ddl_search_path: ?*const @import("../pgwire/search_path.zig").Path = null,
    decision_provider: ?@import("antfly_local_sources").functions_decisions.DecisionProvider = null,
    /// Pgwire retains the original durable owner scope independently of its
    /// mutable, freshly authorized lookup namespace. HTTP leaves this null.
    session_namespace: ?[]const u8 = null,
    inherit_session_database: bool = false,
    inherit_session_namespace: bool = false,
    revision: ?u64 = null,
    target: ?system_catalog.Target = null,
    outcome_transaction_id: ?[32]u8 = null,
    session_id: ?[]const u8 = null,
    /// Durable idle HTTP connection identity, never an alias for session_id.
    connection_id: ?[32]u8 = null,
    connection_revision: ?u64 = null,
    connection_generation: ?u64 = null,
    result_session_id: ?[32]u8 = null,
    transaction_status: @import("antfly_local_sources").sql_session.Status = .idle,
    active_transaction: ?[16]u8 = null,
    accepted_lake_repeatable: bool = false,
    staged: ?*@import("transactions.zig").OwnedTransactionCommitRequest = null,
    range_reads: ?*@import("transactions.zig").OwnedTransactionCommitRequest = null,
    dynamic_snapshot: ?*@import("antfly_local_sources").api_table_read_source.RelationalStatementSnapshot = null,
    dynamic_table: ?[]const u8 = null,
    ranges_staged: bool = false,
    inserting: bool = false,
    prepared_bindings: ?[]const @import("sql_prepared.zig").Binding = null,
    setting_overlay: []const setting_catalog.OverlayEntry = &.{},
    /// Pgwire supplies its own connection-owned overlay. HTTP attached
    /// sessions always read the durable transaction overlay instead.
    setting_overlay_source: SettingOverlaySource = .durable_session,
    expected_setting_epoch: ?u64 = null,
    collect_prepared_bindings: ?*std.ArrayListUnmanaged(@import("sql_prepared.zig").Binding) = null,
    /// One linearizable publication lookup and role capture per table in a
    /// statement. Owner verification still fences every scan against the
    /// exact serving generation, including a publication changed mid-read.
    policy_proofs: ?std.AutoHashMapUnmanaged(u64, ?[]u8) = null,

    pub fn decisionRuntime(self: *Adapter) !?@import("antfly_local_sources").functions_runtime.Runtime {
        if (self.server.cfg.node_config) |config| {
            if (config.registry.decider_configs.count() > 0) {
                const normalized = try self.context.platformDeadline();
                return .{
                    .registry = &config.registry,
                    .io = self.server.embedding_provider_runtime.io,
                    .parallel_work = self.runnable == null and !self.serial_execution,
                    .parent_execution = if (self.runnable != null) .{
                        .ptr = self,
                        .checkpoint = checkpoint,
                        .resource_wait = .{ .begin = spillWaitBegin, .end = spillWaitEnd },
                    } else null,
                    .http = try self.server.embedding_provider_runtime.httpClient(),
                    .context = .{ .io = self.server.embedding_provider_runtime.io, .deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation, .runnable = self.providerRunnable() },
                    .secret_store = self.server.cfg.secret_store,
                    .antfly_provider = self.server.antfly_provider,
                    .antfly_url = config.inference.api_url,
                    .antfly_api_key = config.inference.api_key,
                };
            }
        }
        return null;
    }

    pub fn execute(self: *Adapter, alloc: std.mem.Allocator, compiled: *const @import("antfly_local_sources").sql_compiler.Compiled, parameters: []const std.json.Value, limits: @import("antfly_local_sources").sql_runtime.Limits, guarded_backend: ?catalog.Backend) !@import("antfly_local_sources").sql_runtime.Result {
        const previous_receipt_wait_budget = self.receipt_wait_budget;
        self.receipt_wait_budget = .{};
        defer self.receipt_wait_budget = previous_receipt_wait_budget;
        const previous_visibility = self.lake_visibility;
        try self.read_requirements.validate(.sql);
        if (self.read_requirements.visibility) |visibility| self.lake_visibility = if (visibility == .accepted) .accepted else .committed;
        const previous_context = self.context;
        defer self.lake_visibility = previous_visibility;
        defer self.context = previous_context;
        var write_team: ?@import("antfly_runtime_abi").runnable_abi.Team = null;
        var read_team: ?@import("antfly_runtime_abi").runnable_abi.Team = null;
        const previous_team = self.context.execution_team;
        defer self.context.execution_team = previous_team;
        const participates_in_commit = switch (compiled.statement) {
            .insert, .update, .delete, .merge, .commit => true,
            else => false,
        };
        if (previous_team == null and participates_in_commit) {
            if (self.write_execution) |execution| {
                write_team = try self.writeTeam(execution);
                self.context.execution_team = &write_team.?;
            }
        }
        if (previous_team == null and (compiled.statement == .select or compiled.statement == .explain)) {
            if (self.providerRunnable()) |borrow| {
                // Local retrieval callbacks borrow this same read owner. Its
                // adapter revalidates the read fence on the coordinator, while
                // child capsules retain independent worker state until joining.
                read_team = .{ .borrow = borrow, .maximum_slots = borrow.cpu_slots, .allocator = self.server.alloc };
                self.context.execution_team = &read_team.?;
            }
        }
        if (self.runnable != null and (compiled.statement == .select or compiled.statement == .explain)) try self.beginReadFence();
        defer if (!self.retain_read_guard) self.endReadFence();
        const state_alloc = if (@import("sql_control.zig").isControl(compiled.statement)) self.server.controlAllocator() else self.server.alloc;
        var zig017_return_error: ?anyerror = null;
        var decision_runtime: ?@import("antfly_local_sources").functions_runtime.Runtime = null;
        const previous_provider = self.decision_provider;
        defer self.decision_provider = previous_provider;
        decision_runtime = try self.decisionRuntime();
        defer if (decision_runtime) |*active| active.deinit();
        if (decision_runtime) |*active| self.decision_provider = active.provider();
        if (self.policy_proofs != null) return zig017_failure: {
            zig017_return_error = error.InvalidSqlBackendResponse;
            break :zig017_failure error.InvalidSqlBackendResponse;
        };
        self.policy_proofs = .empty;
        defer {
            var proofs = &self.policy_proofs.?;
            var values = proofs.valueIterator();
            while (values.next()) |value| if (value.*) |token| state_alloc.free(token);
            proofs.deinit(state_alloc);
            self.policy_proofs = null;
        }
        const session_api = @import("sql_session.zig");
        const sessions = @import("antfly_local_sources").sql_session;
        const previous_database = self.database;
        const previous_namespace = self.namespace;
        const previous_setting_overlay = self.setting_overlay;
        var inherited: ?@import("transactions.zig").SessionRegistry.SqlState = null;
        defer {
            self.database = previous_database;
            self.namespace = previous_namespace;
            self.setting_overlay = previous_setting_overlay;
            if (inherited) |*state| state.deinit(state_alloc);
        }
        if (self.session_id) |encoded| {
            const id = @import("distributed_txn.zig").parseTxnIdHex(encoded) catch return zig017_failure: {
                zig017_return_error = error.SqlTransactionNotActive;
                break :zig017_failure error.SqlTransactionNotActive;
            };
            if ((self.server.txn_sessions.principalAccess(state_alloc, id, http_server.transactionPrincipal(self.identity.*)) catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            }) != .allowed) return zig017_failure: {
                zig017_return_error = error.SqlTransactionNotActive;
                break :zig017_failure error.SqlTransactionNotActive;
            };
            inherited = ((self.server.txn_sessions.getSqlState(state_alloc, id) catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            })) orelse return zig017_failure: {
                zig017_return_error = error.SqlTransactionNotActive;
                break :zig017_failure error.SqlTransactionNotActive;
            };
            if (inherited.?.metadata.accepted_lake_reads) self.lake_visibility = .accepted;
            if (!std.meta.eql(inherited.?.connection_id, self.connection_id)) return zig017_failure: {
                zig017_return_error = error.SqlConnectionNotFound;
                break :zig017_failure error.SqlConnectionNotFound;
            };
            if (self.inherit_session_database) self.database = inherited.?.metadata.database;
            if (self.inherit_session_namespace) self.namespace = inherited.?.metadata.namespace;
            self.setting_overlay = (attachedSettingOverlay(self.setting_overlay_source, self.setting_overlay, inherited.?.setting_active.items) catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            });
        }
        var coordinator = session_api.Coordinator{ .server = self.server, .identity = self.identity, .context = self.context };
        var owner = session_api.Adapter{ .alloc = state_alloc, .registry = &self.server.txn_sessions, .node_id = self.server.localSessionNodeId(), .commit_context = &coordinator, .commit_fn = session_api.Coordinator.commit, .supports_range_guards = supportsRangeGuards(self.server), .connection_id = self.connection_id, .connection_revision = self.connection_revision, .connection_overlay = self.setting_overlay };
        var session = sessions.Session{ .owner = owner.owner(), .scope = .{ .principal = http_server.transactionPrincipal(self.identity.*) orelse "", .database = self.database, .namespace = self.session_namespace orelse self.namespace } };
        if (self.session_id) |encoded| {
            const id = @import("distributed_txn.zig").parseTxnIdHex(encoded) catch return zig017_failure: {
                zig017_return_error = error.SqlTransactionNotActive;
                break :zig017_failure error.SqlTransactionNotActive;
            };
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
        errdefer if (zig017_return_error) |err| if (err != error.SqlWriteCapacityUnavailable) session.statementFailed() catch {};
        if ((compiled.statement == .select or compiled.statement == .explain) and self.read_requirements.visibility == .committed and inherited != null and inherited.?.metadata.accepted_lake_reads) return zig017_failure: {
            zig017_return_error = error.ReadSnapshotTooOld;
            break :zig017_failure error.ReadSnapshotTooOld;
        };
        if (compiled.statement == .set_constraints) {
            if (parameters.len != 0) return zig017_failure: {
                zig017_return_error = error.InvalidSqlParameters;
                break :zig017_failure error.InvalidSqlParameters;
            };
            const id = session.transaction_id orelse return zig017_failure: {
                zig017_return_error = error.SqlTransactionNotActive;
                break :zig017_failure error.SqlTransactionNotActive;
            };
            var result = (@import("antfly_local_sources").sql_runtime.Result.empty(alloc, "SET CONSTRAINTS") catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            });
            errdefer result.deinit();
            const timing = compiled.statement.set_constraints;
            (@import("sql_constraint_timing.zig").set(self.server, alloc, self.identity.*, self.context, session.scope, self.namespace, id, timing.names, timing.deferred) catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            });
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
            if (parameters.len != 0) return zig017_failure: {
                zig017_return_error = error.InvalidSqlParameters;
                break :zig017_failure error.InvalidSqlParameters;
            };
            const control_lease = if (session.transaction_id != null and command != .commit)
                self.server.txn_sessions.tryAcquireCommitExecution(session.transaction_id.?) orelse return zig017_failure: {
                    zig017_return_error = error.SqlWriteCapacityUnavailable;
                    break :zig017_failure error.SqlWriteCapacityUnavailable;
                }
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
            var result = (@import("antfly_local_sources").sql_runtime.Result.empty(alloc, tag) catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            });
            errdefer result.deinit();
            const already_failed = command == .commit and ((session.status() catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            })) == .failed;
            const outcome = (session.execute(command) catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            });
            if (outcome.commit) |commit| {
                self.outcome_transaction_id = std.fmt.bytesToHex(commit.reconciliation_id, .lower);
                if (commit.outcome == .unknown) return zig017_failure: {
                    zig017_return_error = error.SqlTransactionOutcomeUnknown;
                    break :zig017_failure error.SqlTransactionOutcomeUnknown;
                };
                if (commit.outcome == .aborted and !already_failed) return zig017_failure: {
                    zig017_return_error = error.SqlWriteConflict;
                    break :zig017_failure error.SqlWriteConflict;
                };
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
        const captured_conflict = compiled.statement == .insert and compiled.statement.insert.conflict != null and compiled.statement.insert.conflict.?.capture_count != 0;
        const deferred_conflict = compiled.statement == .insert and compiled.statement.insert.conflict != null and compiled.statement.insert.conflict.?.deferred_count != 0;
        const implicit_guarded = (compiled.statement == .merge or captured_conflict or deferred_conflict) and session.transaction_id == null;
        if (implicit_guarded) {
            if (!supportsRangeGuards(self.server)) return zig017_failure: {
                zig017_return_error = error.SqlRangeTrackingRequired;
                break :zig017_failure error.SqlRangeTrackingRequired;
            };
            _ = (session.execute(.{ .begin = .{} }) catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            });
        }
        var implicit_decided = false;
        errdefer if (implicit_guarded and !implicit_decided) {
            _ = session.execute(.rollback) catch session.deinit();
        };
        if (session.transaction_id != null) {
            const transaction = (session.statement(switch (compiled.statement) {
                .select, .explain => false,
                else => true,
            }) catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            });
            if ((compiled.statement == .merge or captured_conflict or deferred_conflict) and transaction.isolation == .read_committed) return zig017_failure: {
                zig017_return_error = error.SqlRangeTrackingRequired;
                break :zig017_failure error.SqlRangeTrackingRequired;
            };
            // DDL cannot bypass the native transaction's atomicity boundary.
            (rejectTransactionalDdl(compiled.statement) catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            });
            const id = session.transaction_id.?;
            var result = statement: {
                const execution_lease = self.server.txn_sessions.tryAcquireCommitExecution(id) orelse return zig017_failure: {
                    zig017_return_error = error.SqlWriteCapacityUnavailable;
                    break :zig017_failure error.SqlWriteCapacityUnavailable;
                };
                defer execution_lease.release();
                // A concurrent HTTP SET/RESET may have committed after the
                // initial session attachment but before this statement's
                // execution lease. Refresh the overlay while holding the
                // same lease that serializes session setting mutations.
                var leased_settings: ?@import("transactions.zig").SessionRegistry.SqlState = null;
                defer if (leased_settings) |*state| state.deinit(state_alloc);
                if (self.session_id != null and compiled.uses_current_setting) {
                    self.setting_overlay = (refreshAttachedSettingOverlay(&self.server.txn_sessions, state_alloc, id, self.setting_overlay_source, previous_setting_overlay, &leased_settings) catch |zig017_err| {
                        zig017_return_error = zig017_err;
                        return zig017_err;
                    });
                }
                _ = (session.statement(switch (compiled.statement) {
                    .select, .explain => false,
                    else => true,
                }) catch |zig017_err| {
                    zig017_return_error = zig017_err;
                    return zig017_err;
                });
                var staged = (self.server.txn_sessions.cloneSqlStaged(alloc, id) catch |zig017_err| {
                    zig017_return_error = zig017_err;
                    return zig017_err;
                });
                defer staged.deinit(alloc);
                var read_guards: @import("transactions.zig").OwnedTransactionCommitRequest = .{};
                defer read_guards.deinit(state_alloc);
                self.range_reads = if (transaction.isolation != .read_committed) &read_guards else null;
                self.ranges_staged = false;
                self.active_transaction = id;
                self.accepted_lake_repeatable = self.lake_visibility == .accepted and (transaction.isolation != .serializable or transaction.mode == .read_only);
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
                    self.accepted_lake_repeatable = false;
                    self.staged = null;
                    self.range_reads = null;
                    self.ranges_staged = false;
                    self.inserting = false;
                    self.dynamic_snapshot = null;
                    self.dynamic_table = null;
                }
                var dynamic: ?@import("antfly_local_sources").api_table_read_source.RelationalStatementSnapshot = null;
                defer if (dynamic) |snapshot| snapshot.deinit();
                var dynamic_scratch = std.heap.ArenaAllocator.init(alloc);
                defer dynamic_scratch.deinit();
                if (deferred_conflict) {
                    const table = (resolve(self, dynamic_scratch.allocator(), compiled.statement.insert.table, .read_write) catch |zig017_err| {
                        zig017_return_error = zig017_err;
                        return zig017_err;
                    });
                    if (table.storage_mode != .relational) return zig017_failure: {
                        zig017_return_error = error.UnsupportedSqlExecution;
                        break :zig017_failure error.UnsupportedSqlExecution;
                    };
                    const reads = self.server.table_reads orelse return zig017_failure: {
                        zig017_return_error = error.SqlStatementSnapshotRequired;
                        break :zig017_failure error.SqlStatementSnapshotRequired;
                    };
                    // The immutable cut is captured before INSERT-source and
                    // conflict-owner reads. Unsupported distributed ownership
                    // refuses this capability rather than mixing snapshots.
                    dynamic = self.captureDynamicStatement(alloc, reads, table.physical_name, table.schema_version) catch |zig017_err| {
                        zig017_return_error = zig017_err;
                        return zig017_err;
                    };
                    if (dynamic.?.vtable.open_guarded == null and dynamic.?.vtable.open_guarded_with_admission == null) return zig017_failure: {
                        zig017_return_error = error.SqlRangeTrackingRequired;
                        break :zig017_failure error.SqlRangeTrackingRequired;
                    };
                    self.dynamic_snapshot = &dynamic.?;
                    self.dynamic_table = table.physical_name;
                }
                var active_backend = guarded_backend orelse self.backend();
                active_backend.decision_provider = self.decision_provider;
                if (compiled.uses_current_setting and active_backend.setting_capture == null) active_backend.setting_capture = self.settingCapture();
                if (guarded_backend != null) {
                    active_backend.atomic_statement_read_set = self.range_reads != null;
                    active_backend.coordinated_point_reads = self.range_reads != null;
                    active_backend.coordinated_index_reads = self.range_reads != null and staged.tables.len == 0;
                    active_backend.dynamic_statement_read_set = self.dynamic_snapshot != null;
                }
                var output = (@import("antfly_local_sources").sql_runtime.execute(alloc, active_backend, compiled, parameters, limits) catch |zig017_err| {
                    zig017_return_error = zig017_err;
                    return zig017_err;
                });
                errdefer output.deinit();
                if (!self.ranges_staged and read_guards.tables.len != 0) {
                    _ = ((self.server.txn_sessions.stage(state_alloc, id, &read_guards) catch |zig017_err| {
                        zig017_return_error = zig017_err;
                        return zig017_err;
                    })) orelse return zig017_failure: {
                        zig017_return_error = error.SqlTransactionNotActive;
                        break :zig017_failure error.SqlTransactionNotActive;
                    };
                }
                output.output.mutation_outcome = null;
                break :statement output;
            };
            errdefer result.deinit();
            if (implicit_guarded) {
                // The statement lease and borrowed read view are gone before
                // commit admission. A decided/unknown outcome is never retried.
                const decision = (session.execute(.commit) catch |zig017_err| {
                    zig017_return_error = zig017_err;
                    return zig017_err;
                });
                const commit = decision.commit orelse return zig017_failure: {
                    zig017_return_error = error.InvalidSqlBackendResponse;
                    break :zig017_failure error.InvalidSqlBackendResponse;
                };
                implicit_decided = true;
                self.outcome_transaction_id = std.fmt.bytesToHex(commit.reconciliation_id, .lower);
                result.output.mutation_outcome = switch (commit.outcome) {
                    .committed => .committed,
                    .committed_pending => .committed_pending,
                    .committed_repair_required => .committed_repair_required,
                    .aborted => return zig017_failure: {
                        zig017_return_error = error.SqlWriteConflict;
                        break :zig017_failure error.SqlWriteConflict;
                    },
                    .unknown => return zig017_failure: {
                        zig017_return_error = error.SqlTransactionOutcomeUnknown;
                        break :zig017_failure error.SqlTransactionOutcomeUnknown;
                    },
                };
            }
            self.validateReadFence(true) catch |err| {
                zig017_return_error = err;
                return err;
            };
            return result;
        }
        var statement_backend = guarded_backend orelse self.backend();
        statement_backend.decision_provider = self.decision_provider;
        if (compiled.uses_current_setting and statement_backend.setting_capture == null) statement_backend.setting_capture = self.settingCapture();
        if (self.read_delivery) |delivery| if (self.session_id == null and compiled.statement == .select) {
            if (try @import("antfly_local_sources").sql_read_stream.Stream.open(alloc, statement_backend, compiled, parameters, limits)) |stream| {
                defer stream.close();
                var delivered = try delivery.deliver(delivery.ptr, alloc, stream);
                errdefer delivered.deinit();
                try self.validateReadFence(true);
                return delivered;
            }
        };
        var result = @import("antfly_local_sources").sql_runtime.execute(alloc, statement_backend, compiled, parameters, limits) catch |zig017_err| {
            zig017_return_error = zig017_err;
            return zig017_err;
        };
        errdefer result.deinit();
        self.validateReadFence(true) catch |err| {
            zig017_return_error = err;
            return err;
        };
        return result;
    }

    pub fn beginReadFence(self: *Adapter) !void {
        if (self.read_guard != null) return error.InvalidReadFenceTransition;
        self.read_guard = read_fence.Guard.init(self.server.alloc);
    }

    pub fn reserveReadValidationMemory(self: *Adapter, bytes: usize) !void {
        if (self.read_policy_memory != null or self.read_guard == null or self.read_guard.?.validating) return error.InvalidReadFenceTransition;
        const execution = self.runnable orelse return error.InvalidRunnableTransition;
        try execution.checkLifetime();
        try self.context.ensureActive();
        const entry = execution.entry orelse return error.InvalidRunnableTransition;
        if (entry.state != .running or entry.class != .query or entry.slots != 1 or entry.parent != null or entry.children != 0 or
            entry.cpu_borrow_budget != null or entry.cleanup_active or entry.completion_active or entry.protected_cleanup_root or
            entry.read_lane == .bounded or entry.read_transition_active or entry.completion_options != null)
            return error.InvalidRunnableTransition;
        const memory = self.server.request_memory orelse return error.ExecutionWaitUnsupported;
        self.read_policy_memory = try @import("read_policy_memory.zig").Reservation.init(memory.allocator(false), bytes);
        errdefer {
            self.read_policy_memory.?.deinit();
            self.read_policy_memory = null;
        }
        // Refuse insufficient storage before the stage can borrow CPU. This
        // preflights the current policy shape, not every possible future schema.
        try self.validateReadFence(true);
        // Metadata validation may wait in another clock/cancellation domain.
        // Publish the reservation only while the original execution is live.
        try execution.checkLifetime();
    }

    pub fn endReadFence(self: *Adapter) void {
        if (self.read_policy_memory) |*memory| memory.deinit();
        self.read_policy_memory = null;
        if (self.read_guard) |*guard| guard.deinit();
        self.read_guard = null;
    }

    pub fn validateReadFence(self: *Adapter, force: bool) !void {
        const guard = if (self.read_guard) |*value| value else return;
        // A cached guard is authority only within its original live execution.
        // Refusal must also discard a successful check from an earlier call.
        errdefer guard.checked_generation = null;
        const execution = self.runnable orelse return error.InvalidReadFenceTransition;
        try execution.checkLifetime();
        const entry = if (execution.entry) |*value| value else return error.InvalidReadFenceTransition;
        const reserved = if (self.read_policy_memory) |*memory| memory else null;
        const scratch = if (reserved) |memory| try memory.begin() else if (entry.class == .control) self.server.controlAllocator() else self.server.alloc;
        defer if (reserved) |memory| memory.end();
        try guard.check(scratch, execution.resume_generation, force, .{ .ptr = self, .check = validateReadPins });
        try self.context.ensureActive();
        // Catalog/policy IO may use another clock or cancellation domain. Its
        // successful result cannot revive an owner stopped during that wait.
        try execution.checkLifetime();
    }

    fn captureReadFence(self: *Adapter, table: catalog.Table) !void {
        const guard = if (self.read_guard) |*value| value else return;
        const scope = table.scope orelse return error.InvalidSqlBackendResponse;
        const filter = try http_server.resolveEffectiveRowFilterJson(guard.alloc, self.identity.*, table.physical_name);
        defer if (filter) |value| guard.alloc.free(value);
        const policy = try self.readPolicyGeneration(guard.alloc, table.id, table.schema_version);
        try guard.capture(.{ .database = scope.database, .namespace = scope.namespace, .name = scope.name, .physical = table.physical_name, .id = table.id, .schema_version = table.schema_version, .catalog_revision = scope.revision, .filter = filter, .row_policy = policy });
        try self.validateReadFence(false);
    }

    fn readPolicyGeneration(self: *Adapter, alloc: std.mem.Allocator, table_id: u64, schema_version: u32) !?read_fence.Table.Policy {
        var context = self.context;
        context.row_policy_install_authority = true;
        const bytes = try self.server.source.systemCatalog(alloc, context, .{ .policy_publication_status = table_id });
        defer alloc.free(bytes);
        var parsed = try std.json.parseFromSlice(?@import("antfly_local_sources").system_catalog_policies.PublicationStamp, alloc, bytes, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        const publication = parsed.value orelse return null;
        if (publication.table_id != table_id or publication.schema_version != schema_version) return error.RowPolicyCatalogChanged;
        const serving = (try publication.servingAuthority()) orelse return null;
        return .{ .generation = serving.generation, .catalog_epoch = serving.catalog_epoch };
    }

    fn validateReadPins(raw: *anyopaque, alloc: std.mem.Allocator, tables: []const read_fence.Table) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        // Only actual validation crosses the prepared-stage boundary. Cached
        // checks perform no catalog IO and must not refresh an ordinary quantum.
        const execution = self.runnable orelse return error.InvalidReadFenceTransition;
        try execution.endCpuBorrowStage();
        try self.context.ensureActive();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        const targets = try scratch.alloc(system_catalog.Target, tables.len);
        const revision = tables[0].catalog_revision;
        for (tables, targets) |table, *target| {
            if (table.catalog_revision != revision) return error.CatalogGenerationChanged;
            target.* = .{ .database = table.database, .namespace = table.namespace, .table = table.name };
        }
        const bytes = try self.server.source.systemCatalog(scratch, self.context, .{ .resolve_many = .{ .targets = targets, .include_query_definitions = true, .expected_revision = revision } });
        const current = try std.json.parseFromSliceLeaky(system_catalog.ResolvedMany, scratch, bytes, .{});
        if (current.revision != revision or current.tables.len != tables.len) return error.CatalogGenerationChanged;
        for (tables, current.tables) |pinned, resolved| {
            const table = resolved orelse return error.CatalogGenerationChanged;
            if (table.table_id != pinned.id or !std.mem.eql(u8, table.name, pinned.physical)) return error.CatalogGenerationChanged;
            const definition = table.query_definition orelse return error.InvalidSqlBackendResponse;
            if (definition.table_id != pinned.id) return error.CatalogGenerationChanged;
            const binding = if (self.read_policy_memory != null)
                try @import("sql_schema_cache.zig").Cache.resolveUncached(self.server.sqlPlanCacheIo(), scratch, definition.schema_json, table.table_id, table.name)
            else
                try self.server.sql_schema_cache.resolve(self.server.sqlPlanCacheIo(), scratch, definition.schema_json, table.table_id, table.name);
            if (binding.schema_version != pinned.schema_version) return error.CatalogGenerationChanged;
            const policy = try self.readPolicyGeneration(scratch, pinned.id, pinned.schema_version);
            if (!std.meta.eql(policy, pinned.row_policy)) return error.RowPolicyCatalogChanged;
        }
        // Catalog I/O may itself wait. Capture current user/key policy after
        // that lookup so the policy preceding the wait cannot authorize it.
        if (self.identity.*) |admitted| {
            var fresh: http_server.AuthenticatedIdentity = undefined;
            if (admitted.live_user_manager) |manager| {
                if (!admitted.is_internal_service) {
                    var snapshot = manager.snapshotCredentialPoliciesAlloc(scratch, admitted.credential_principal) catch |err| switch (err) {
                        error.OutOfMemory => return error.OutOfMemory,
                        else => return error.Forbidden,
                    };
                    defer snapshot.deinit(scratch);
                    if (!std.mem.eql(u8, admitted.username, snapshot.policy.username)) return error.Forbidden;
                    if (admitted.user_instance_id) |instance| if (!std.mem.eql(u8, &instance, &snapshot.instance_id)) return error.Forbidden;
                    // Native row-policy proofs can use roles and metadata
                    // even when no user row filter exists. An already opened
                    // cursor cannot reinterpret buffered rows with new values.
                    const protected = for (tables) |table| {
                        if (table.row_policy != null) break true;
                    } else false;
                    if (protected) {
                        if (!std.mem.eql(u8, admitted.metadata_json, snapshot.policy.metadata_json) or
                            admitted.roles.len != snapshot.policy.roles.len) return error.Forbidden;
                        for (admitted.roles, snapshot.policy.roles) |old, current_role| {
                            if (!std.mem.eql(u8, old, current_role)) return error.Forbidden;
                        }
                    }
                    fresh = (try http_server.cloneCatalogIdentity(scratch, .{
                        .user_instance_id = snapshot.instance_id,
                        .username = snapshot.policy.username,
                        .credential_principal = admitted.credential_principal,
                        .permissions = snapshot.policy.permissions,
                        .row_filter = snapshot.policy.row_filter,
                        .metadata_json = snapshot.policy.metadata_json,
                        .roles = snapshot.policy.roles,
                        .live_user_manager = manager,
                    })).?;
                } else fresh = (try http_server.cloneCatalogIdentity(scratch, admitted)).?;
            } else fresh = (try http_server.cloneCatalogIdentity(scratch, admitted)).?;
            defer fresh.deinit(scratch);
            for (tables, targets) |pinned, target| {
                const logical = try target.resourceNameAlloc(scratch);
                if (!http_server.permissionsAllow(admitted.permissions, .table, logical, .read) or
                    !http_server.permissionsAllow(fresh.permissions, .table, logical, .read)) return error.Forbidden;
                try http_server.projectCatalogIdentity(scratch, &fresh, logical, pinned.physical);
                const filter = try http_server.resolveEffectiveRowFilterJson(scratch, fresh, pinned.physical);
                if ((filter == null) != (pinned.filter == null) or
                    (filter != null and !std.mem.eql(u8, filter.?, pinned.filter.?))) return error.Forbidden;
            }
        }
        try self.context.ensureActive();
    }

    fn cpuScope(self: *Adapter) ?@import("antfly_local_sources").sql_parallel_scheduler.ExecutionScope {
        if (self.serial_execution) return null;
        const coordinator = self.coordinatorScope() orelse return null;
        return .{ .execution = coordinator.execution, .allocator = coordinator.allocator, .io = self.server.embedding_provider_runtime.io, .wait = .{ .ptr = self, .begin = spillWaitBegin, .end = spillWaitEnd }, .checkpoint = checkpoint, .scratch = coordinator.scratch };
    }

    fn coordinatorScope(self: *Adapter) ?@import("antfly_local_sources").sql_parallel_scheduler.CoordinatorScope {
        if (self.read_guard == null) return null;
        const execution = self.runnable orelse return null;
        const entry = execution.entry orelse return null;
        if (entry.class != .query or entry.slots != 1 or entry.cleanup_active or entry.completion_active) return null;
        const memory = self.server.request_memory orelse return null;
        return .{
            .execution = execution,
            .allocator = memory.allocator(false),
            .check = .{ .context = self, .check = checkpoint },
            .scratch = .{ .context = self, .acquire = acquireNumericScratch },
        };
    }

    const NumericScratch = struct {
        const Budget = @import("../common/allocation_budget.zig");
        metadata: std.mem.Allocator,
        fixed: Budget.Fixed,
        chosen: bool = false,

        fn probe(_: *anyopaque) callconv(.c) c_int {
            // The serial alternative needs no incremental scratch. Both
            // alternatives use the same paid original CPU continuation.
            return @backingInt(http_server.RunnableScheduler.GrantResources.Probe.ready);
        }
        fn acquire(raw: *anyopaque) callconv(.c) c_int {
            const self: *NumericScratch = @ptrCast(@alignCast(raw));
            std.debug.assert(!self.chosen);
            self.chosen = true;
            _ = self.fixed.tryAcquire();
            return @backingInt(http_server.RunnableScheduler.GrantResources.Acquire.acquired);
        }
        fn release(raw: *anyopaque) void {
            const self: *NumericScratch = @ptrCast(@alignCast(raw));
            self.fixed.deinit();
            self.metadata.destroy(self);
        }
    };

    fn acquireNumericScratch(raw: *anyopaque, bytes: usize) !?@import("antfly_local_sources").sql_parallel_scheduler.ScratchLease {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const scope = self.coordinatorScope() orelse return null;
        const memory = self.server.request_memory orelse return null;
        const ticket = scope.allocator.create(NumericScratch) catch return null;
        ticket.* = .{ .metadata = scope.allocator, .fixed = .{ .budget = &memory.foreground, .bytes = bytes } };
        var transferred = false;
        defer if (!transferred) NumericScratch.release(ticket);
        try scope.checkpointCurrent();
        // Queue restrictions cannot run a CPU checkpoint while parked. The
        // original lifetime is checked by enter; validate full read policy on
        // the running coordinator immediately afterwards, before any math.
        try scope.execution.enterWithResources(.{ .context = ticket, .probe = NumericScratch.probe, .acquire = NumericScratch.acquire }, .{ .context = self, .check = numericScratchQueueCheck });
        try scope.checkpointCurrent();
        if (!ticket.fixed.reserved) return null;
        transferred = true;
        return .{ .allocator = ticket.fixed.allocator(), .context = ticket, .release = NumericScratch.release };
    }

    fn numericScratchQueueCheck(raw: *anyopaque) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
    }

    pub fn backend(self: *Adapter) catalog.Backend {
        // Each new backend starts an execution scope; binding, cursor selection
        // and fallback execution within that scope share the receipt budget.
        self.receipt_wait_budget = .{};
        return .{ .execution_binding_validator = if (self.read_requirements.after_write != null) .{ .ptr = self, .validate = validateReadReceiptBinding } else null, .scalar_control = .{ .ptr = self, .checkpoint = scalarCheckpoint }, .cpu_scope = self.cpuScope(), .coordinator_scope = self.coordinatorScope(), .execution_io = if (self.runnable == null and !self.serial_execution) self.server.embedding_provider_runtime.io else null, .spill_io = if (self.runnable != null or self.serial_execution) self.server.embedding_provider_runtime.io else null, .spill_wait = if (self.runnable != null) .{ .ptr = self, .begin = spillWaitBegin, .end = spillWaitEnd } else null, .ptr = self, .decision_provider = self.decision_provider, .predicate_only_mutations = true, .atomic_statement_read_set = self.active_transaction != null and self.range_reads != null, .coordinated_point_reads = self.active_transaction != null and self.range_reads != null, .coordinated_index_reads = self.active_transaction != null and self.range_reads != null and (self.staged == null or self.staged.?.tables.len == 0), .dynamic_statement_read_set = self.dynamic_snapshot != null, .vtable = &.{ .resolve_conflict_owners = resolveConflictOwners, .generate_row_id = generateRowId, .resolve = resolve, .scan = scan, .supports_scan_order = true, .open_scan = openScan, .aggregate_partials = openAggregatePartials, .open_statement = openStatement, .estimate_scans = estimateScans, .mutate = mutate, .mutate_prepared = mutatePrepared, .prepare_mutations = prepareMutations, .ddl = ddl, .checkpoint = checkpoint } };
    }

    pub fn settingCapture(self: *Adapter) @FieldType(catalog.Backend, "setting_capture") {
        return .{ .owner = .{ .ptr = self, .load = loadSettings }, .scope = .{ .principal = http_server.transactionPrincipal(self.identity.*) orelse "", .database = self.database }, .overlay = self.setting_overlay };
    }

    fn loadSettings(ptr: *anyopaque, alloc: std.mem.Allocator, scope: setting_catalog.Scope) !setting_catalog.RawSnapshot {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        if (scope.principal.len == 0 or !std.mem.eql(u8, scope.principal, http_server.transactionPrincipal(self.identity.*) orelse "") or !std.mem.eql(u8, scope.database, self.database)) return error.Forbidden;
        var context = self.context;
        context.setting_read_principal = scope.principal;
        const bytes = try self.server.source.systemCatalog(alloc, context, .{ .setting_snapshot = scope });
        defer alloc.free(bytes);
        const snapshot = try std.json.parseFromSliceLeaky(setting_catalog.RawSnapshot, alloc, bytes, .{ .allocate = .alloc_always });
        if (self.expected_setting_epoch) |epoch| if (snapshot.epoch != epoch) return error.SettingCatalogChanged;
        return snapshot;
    }

    fn generateRowId(ptr: *anyopaque, alloc: std.mem.Allocator) ![]const u8 {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        try self.context.ensureActive();
        return @import("antfly_local_sources").storage_row_identity.generate(alloc, self.server.sharedApiIo() orelse return error.UnsupportedSqlExecution);
    }

    fn resolveConflictOwners(ptr: *anyopaque, alloc: std.mem.Allocator, scratch_alloc: std.mem.Allocator, table: catalog.Table, target: catalog.ConflictTarget, mutations: []const catalog.Mutation) ![]const catalog.ConflictOwner {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        try self.verify(alloc, table);
        const integrity = @import("antfly_local_sources").api_relational_integrity_commit;
        const predicates = try @import("antfly_local_sources").sql_conflict_predicate.toNative(alloc, target.conditions);
        const keys = try @import("antfly_local_sources").sql_conflict_predicate.expressionsToNative(alloc, target.expressions);
        var snapshot = (try self.server.source.adminSnapshot()) orelse return error.IntegrityCatalogUnavailable;
        defer self.server.source.freeAdminSnapshot(&snapshot);
        const writes = try @import("antfly_local_sources").sql_mutation_images.writes(db_types.BatchWrite, alloc, mutations);
        const previous_view = if (self.staged) |staged| try staged.distributedTables(alloc) else null;
        defer if (previous_view) |view| view.deinit();
        const previous = if (previous_view) |view| view.tables else &.{};
        const owners = try integrity.resolveConflictTargetOwnersWithScratch(alloc, scratch_alloc, self.server.table_reads orelse return error.UnsupportedSqlExecution, snapshot.tables, snapshot.ranges, table.physical_name, table.schema_version, .{ .columns = target.columns, .expressions = keys, .predicate = predicates, .constraint_name = target.constraint_name }, writes, previous, self.context);
        try self.verify(alloc, table);
        const result = try alloc.alloc(catalog.ConflictOwner, owners.len);
        for (owners, result) |*owner, *out| out.* = .{ .key = owner.key, .identity = owner.identity, .identities = owner.identities, .guard = owner };
        return result;
    }

    fn conflictGuards(alloc: std.mem.Allocator, mutations: []const catalog.Mutation) !?@import("transactions.zig").TableCommitRequest.ConflictGuards {
        const integrity = @import("antfly_local_sources").api_relational_integrity_commit;
        const Command = @import("antfly_local_sources").storage_db_relational_integrity_contract.Command;
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

    fn ddl(ptr: *anyopaque, allocators: catalog.DdlAllocators, input: catalog.Ddl) !catalog.DdlOutcome {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        return @import("sql_catalog.zig").executeWithAllocators(self.server, self.identity.*, self.context, self.database, self.namespace, allocators, input, self.ddl_search_path);
    }

    fn activeCleanup(self: *Adapter) !?@import("antfly_runtime_abi").runnable_abi.Borrowed {
        const execution = self.runnable orelse return null;
        const entry = execution.entry orelse return null;
        if (!entry.cleanup_active) return null;
        const phase = if (self.cleanup_phase) |*value| value else return error.InvalidRunnableTransition;
        return try phase.borrowed();
    }

    fn checkCleanupStatus(status: @import("antfly_runtime_abi").error_abi.Status) !void {
        if (!status.isOk()) return @import("antfly_runtime_abi").error_abi.errorFromStatus(status);
    }

    fn spillWaitBegin(ptr: *anyopaque) !void {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        if (try self.activeCleanup()) |borrow| {
            try checkCleanupStatus(borrow.checkpoint(borrow.context));
            try checkCleanupStatus(borrow.park(borrow.context, @backingInt(@import("antfly_runtime_abi").runnable_abi.Wait.resource)));
            return;
        }
        try self.context.ensureActive();
        const execution = self.runnable orelse return error.InvalidRunnableTransition;
        try execution.park(.resource_blocked);
    }

    fn writeTeam(self: *Adapter, execution: *http_server.RunnableScheduler.Execution) !@import("antfly_runtime_abi").runnable_abi.Team {
        if (self.write_completion) |completion| {
            if (completion.execution != execution) return error.InvalidRunnableTransition;
            return completion.team();
        }
        const entry = execution.entry orelse return error.InvalidRunnableTransition;
        // Concurrent children cannot borrow a request arena. This stable
        // process-owned partition enforces the API request byte limit.
        const memory = self.server.request_memory orelse return error.ExecutionWaitUnsupported;
        return @import("runnable_write_team.zig").init(execution, memory.allocator(entry.class == .control));
    }

    pub fn providerRunnable(self: *Adapter) ?@import("antfly_runtime_abi").runnable_abi.Borrowed {
        const execution = self.runnable orelse return null;
        const entry = execution.entry orelse return null;
        if (self.cleanup_phase == null) self.cleanup_phase = @import("runnable_local_cleanup.zig").Cleanup.init(execution) catch null;
        const Callback = struct {
            const runtime = @import("antfly_runtime_abi");
            fn cpuBorrow(raw: *anyopaque, options: *const runtime.runnable_abi.CpuBorrow) callconv(.c) runtime.error_abi.Status {
                options.validate() catch |err| return runtime.error_abi.statusFromError(err);
                const adapter: *Adapter = @ptrCast(@alignCast(raw));
                const active_execution = adapter.runnable orelse return runtime.error_abi.statusFromError(error.InvalidRunnableTransition);
                if (options.enabled != 0) {
                    adapter.context.ensureActive() catch |err| return runtime.error_abi.statusFromError(err);
                    if (adapter.read_guard == null) return runtime.error_abi.statusFromError(error.InvalidReadFenceTransition);
                    if (adapter.read_guard.?.tables.items.len != 0 and adapter.read_policy_memory == null)
                        return runtime.error_abi.statusFromError(error.InvalidRunnableTransition);
                    adapter.validateReadFence(false) catch |err| return runtime.error_abi.statusFromError(err);
                    active_execution.enableCpuBorrow(.{ .max_non_yield_ns = options.max_non_yield_ns }) catch |err| return runtime.error_abi.statusFromError(err);
                } else {
                    active_execution.disableCpuBorrow() catch |err| return runtime.error_abi.statusFromError(err);
                    adapter.context.ensureActive() catch |err| return runtime.error_abi.statusFromError(err);
                    adapter.validateReadFence(false) catch |err| return runtime.error_abi.statusFromError(err);
                }
                return .ok;
            }
            fn currentClass(raw: *anyopaque, out: *c_int) callconv(.c) runtime.error_abi.Status {
                const adapter: *Adapter = @ptrCast(@alignCast(raw));
                const active_execution = adapter.runnable orelse return runtime.error_abi.statusFromError(error.InvalidRunnableTransition);
                out.* = @intCast(@backingInt(active_execution.retainedClass() catch |err| return runtime.error_abi.statusFromError(err)));
                return .ok;
            }
            fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
                const adapter: *Adapter = @ptrCast(@alignCast(raw));
                const active_execution = adapter.runnable orelse return runtime.error_abi.statusFromError(error.InvalidRunnableTransition);
                active_execution.checkLifetime() catch |err| return runtime.error_abi.statusFromError(err);
                // Activated cleanup has its own finite fence. Polling the
                // expired foreground context must not cancel that obligation.
                if (!active_execution.entry.?.cleanup_active) adapter.context.ensureActive() catch |err| return runtime.error_abi.statusFromError(err);
                return .ok;
            }
            fn check(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
                const adapter: *Adapter = @ptrCast(@alignCast(raw));
                if (adapter.runnable.?.entry.?.cleanup_active) {
                    const borrow = adapter.cleanup_phase.?.borrowed() catch |err| return runtime.error_abi.statusFromError(err);
                    return borrow.checkpoint(borrow.context);
                }
                checkpoint(raw) catch |err| return runtime.error_abi.statusFromError(err);
                return .ok;
            }
            fn park(raw: *anyopaque, reason: c_int) callconv(.c) runtime.error_abi.Status {
                const adapter: *Adapter = @ptrCast(@alignCast(raw));
                if (adapter.runnable.?.entry.?.cleanup_active) {
                    const borrow = adapter.cleanup_phase.?.borrowed() catch |err| return runtime.error_abi.statusFromError(err);
                    return borrow.park(borrow.context, reason);
                }
                if (reason != @backingInt(runtime.runnable_abi.Wait.resource)) return runtime.error_abi.statusFromError(error.InvalidArgument);
                spillWaitBegin(raw) catch |err| return runtime.error_abi.statusFromError(err);
                return .ok;
            }
            fn enter(raw: *anyopaque, class: c_int) callconv(.c) runtime.error_abi.Status {
                const adapter: *Adapter = @ptrCast(@alignCast(raw));
                if (adapter.runnable.?.entry.?.cleanup_active) {
                    const borrow = adapter.cleanup_phase.?.borrowed() catch |err| return runtime.error_abi.statusFromError(err);
                    return borrow.enter(borrow.context, class);
                }
                const next = std.enums.fromInt(@import("antfly_local_sources").common_request_admission.runnable_scheduler.Class, class) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
                const current = adapter.runnable orelse return runtime.error_abi.statusFromError(error.InvalidRunnableTransition);
                current.enter(next) catch |err| return runtime.error_abi.statusFromError(err);
                adapter.context.ensureActive() catch |err| return runtime.error_abi.statusFromError(err);
                adapter.validateReadFence(false) catch |err| return runtime.error_abi.statusFromError(err);
                return .ok;
            }
            fn enterChecked(raw: *anyopaque, class: c_int, restriction: *const runtime.runnable_abi.Reentry) callconv(.c) runtime.error_abi.Status {
                const adapter: *Adapter = @ptrCast(@alignCast(raw));
                if (adapter.runnable.?.entry.?.cleanup_active) {
                    const borrow = adapter.cleanup_phase.?.borrowed() catch |err| return runtime.error_abi.statusFromError(err);
                    const phase = std.enums.fromInt(runtime.runnable_abi.Class, class) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
                    borrow.enterChecked(phase, restriction) catch |err| return runtime.error_abi.statusFromError(err);
                    return .ok;
                }
                const next = std.enums.fromInt(@import("antfly_local_sources").common_request_admission.runnable_scheduler.Class, class) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
                const current = adapter.runnable orelse return runtime.error_abi.statusFromError(error.InvalidRunnableTransition);
                @import("runnable_reentry.zig").enter(current, next, restriction) catch |err| return runtime.error_abi.statusFromError(err);
                adapter.context.ensureActive() catch |err| return runtime.error_abi.statusFromError(err);
                adapter.validateReadFence(false) catch |err| return runtime.error_abi.statusFromError(err);
                return .ok;
            }
            fn reserveCleanup(raw: *anyopaque, options: *const runtime.runnable_abi.Cleanup) callconv(.c) runtime.error_abi.Status {
                const adapter: *Adapter = @ptrCast(@alignCast(raw));
                adapter.context.ensureActive() catch |err| return runtime.error_abi.statusFromError(err);
                adapter.cleanup_phase.?.reserveChecked(options) catch |err| return runtime.error_abi.statusFromError(err);
                return .ok;
            }
            fn activateCleanup(raw: *anyopaque, out_slots: *usize) callconv(.c) runtime.error_abi.Status {
                const adapter: *Adapter = @ptrCast(@alignCast(raw));
                adapter.cleanup_phase.?.execution.activateCleanup() catch |err| return runtime.error_abi.statusFromError(err);
                out_slots.* = adapter.cleanup_phase.?.execution.entry.?.slots;
                return .ok;
            }
            fn reserveChild(raw: *anyopaque, options: *const runtime.runnable_abi.ChildStart) callconv(.c) runtime.error_abi.Status {
                options.validate() catch |err| return runtime.error_abi.statusFromError(err);
                const adapter: *Adapter = @ptrCast(@alignCast(raw));
                // Capture/revalidate on the coordinator before publication.
                // Children receive independent execution/IO state, never this
                // Adapter's mutable read guard. Operator fanout still requires
                // independent child backend adapters and policy fences.
                checkpoint(raw) catch |err| return runtime.error_abi.statusFromError(err);
                const memory = adapter.server.request_memory orelse return runtime.error_abi.statusFromError(error.ExecutionWaitUnsupported);
                const current = adapter.runnable orelse return runtime.error_abi.statusFromError(error.InvalidRunnableTransition);
                const parent = current.entry orelse return runtime.error_abi.statusFromError(error.InvalidRunnableTransition);
                @import("runnable_local_child.zig").reserve(current, memory.allocator(parent.class == .control), options) catch |err| return runtime.error_abi.statusFromError(err);
                return .ok;
            }
        };
        return .{ .context = self, .cpu_slots = entry.slots, .parent_class = @intCast(@backingInt(entry.class)), .checkpoint = Callback.check, .park = Callback.park, .enter = Callback.enter, .enter_checked = Callback.enterChecked, .reserve_cleanup = if (self.cleanup_phase != null) Callback.reserveCleanup else null, .activate_cleanup = if (self.cleanup_phase != null) Callback.activateCleanup else null, .reserve_child = Callback.reserveChild, .check_lifetime = Callback.lifetime, .current_class = Callback.currentClass, .cpu_borrow = Callback.cpuBorrow };
    }

    fn spillWaitEnd(ptr: *anyopaque) !void {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        if (try self.activeCleanup()) |borrow| {
            try checkCleanupStatus(borrow.enter(borrow.context, borrow.parent_class));
            return;
        }
        const execution = self.runnable orelse return error.InvalidRunnableTransition;
        const class = if (execution.entry) |*entry| entry.class else return error.InvalidRunnableTransition;
        try execution.enter(class);
        try self.context.ensureActive();
        try self.validateReadFence(false);
    }

    fn checkpoint(ptr: *anyopaque) !void {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        if (try self.activeCleanup()) |borrow| {
            try checkCleanupStatus(borrow.checkpoint(borrow.context));
            return;
        }
        try self.context.ensureActive();
        if (self.runnable) |execution| try execution.checkpoint();
        try self.validateReadFence(false);
    }

    fn scalarCheckpoint(ptr: ?*anyopaque) !void {
        // Native scalar frames cannot yield or perform catalog/policy IO.
        // The coordinator owns full read validation at safe boundaries.
        const self: *Adapter = @ptrCast(@alignCast(ptr.?));
        var admission: ReadAdmission = .{ .adapter = self };
        try admission.checkLifetime();
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
        if (binding.external_base_source != null and action != .read and action != .admin) return error.ExternalLakeReadOnly;
        if (binding.external_base_source != null) binding.external_indexes = .{
            .catalog_json = definition.lake_index_catalog_json,
            .object_generation = definition.object_storage_generation,
            .indexes_json = definition.indexes_json,
            .schema_json = definition.schema_json,
            .desired = @import("antfly_local_sources").metadata_lake_index_catalog.desiredFingerprint(.{
                .table_id = table.table_id,
                .name = table.name,
                .schema_json = definition.schema_json,
                .read_schema_json = definition.read_schema_json,
                .indexes_json = definition.indexes_json,
            }),
        };
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
        if (action == .read) try self.captureReadFence(binding);
        return binding;
    }

    fn verify(self: *Adapter, alloc: std.mem.Allocator, table: catalog.Table) !void {
        try self.context.ensureActive();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const temporary = arena.allocator();
        // A renamed/reused logical name never silently retargets a prepared
        // statement. The coordinator still applies its own durable auth fence.
        const scope = table.scope orelse return error.InvalidSqlBackendResponse;
        const target: system_catalog.Target = .{ .database = scope.database, .namespace = scope.namespace, .table = scope.name };
        const bytes = try self.server.source.systemCatalog(temporary, self.context, .{ .resolve_many = .{ .targets = &.{target}, .expected_revision = scope.revision } });
        const snapshot = try std.json.parseFromSliceLeaky(system_catalog.ResolvedMany, temporary, bytes, .{});
        if (snapshot.revision != scope.revision or snapshot.tables.len != 1) return error.CatalogGenerationChanged;
        const current = snapshot.tables[0] orelse return error.CatalogGenerationChanged;
        if (current.table_id != table.id or !std.mem.eql(u8, current.name, table.physical_name)) return error.CatalogGenerationChanged;
    }

    /// Read-only serializable lake transactions serialize immediately before
    /// validation starts. Monotone WAL heads and exact metadata bytes prove
    /// every captured table was unchanged from its read through that point.
    /// Mixed native writes need a lake participant in distributed prepare;
    /// they cannot safely use this read-only validation protocol.
    pub fn validateAcceptedSerializable(self: *Adapter, a: std.mem.Allocator, id: db_types.TxnId) !void {
        var state = (try self.server.txn_sessions.getSqlState(a, id)) orelse return error.SqlTransactionNotActive;
        defer state.deinit(a);
        if (state.metadata.isolation != .serializable or !state.metadata.accepted_lake_reads) return;
        if (state.metadata.mode != .read_only) return error.UnsupportedSqlExecution;
        const entries = try self.server.txn_sessions.cloneLakeCuts(a, id);
        defer {
            for (entries) |*entry| entry.deinit(a);
            a.free(entries);
        }
        if (entries.len == 0) return;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var store = try @import("lake_index_store.zig").Store.openNativeFor(scratch, self.server.cfg.node_config, self.server.cfg.secret_store, false, self.server.cfg.deployment_mode, self.server.cfg.native_lake_artifact_base_dir, try self.lakeReadContext(null));
        defer store.deinit();
        var artifacts = store.artifactStore();
        const identity = try @import("native_retained_cut.zig").storeIdentity(scratch, store.locator);
        const normalized = try self.context.platformDeadline();
        const context: @import("antfly_local_sources").serverless_query_lake_read_context.Context = try self.lakeReadContext(normalized.deadline_ns);
        for (entries) |entry| {
            try self.context.ensureActive();
            const token = try std.json.parseFromSliceLeaky([]const u8, scratch, entry.document_json orelse return error.CatalogGenerationChanged, .{});
            const cut = try @import("lake_sql_retained_cut.zig").loadDescriptorFor(scratch, &artifacts, identity, token, entry.version, @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms, self.context.cancellation, context);
            const table = try resolve(self, scratch, cut.name orelse return error.CatalogGenerationChanged, .read);
            if (table.id != cut.table_id or table.schema_version != cut.schema_version or !std.mem.eql(u8, table.physical_name, entry.table_name) or (if (table.external_indexes) |indexes| indexes.object_generation else 0) != cut.object_generation) return error.SqlWriteConflict;
            const binding = (table.external_base_source orelse return error.SqlWriteConflict).binding;
            const options: @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions = .{ .node_config = self.server.cfg.node_config, .secret_store = self.server.cfg.secret_store, .catalog_table_id = table.id, .catalog_generation = cut.object_generation };
            var current = try @import("../serverless/configured_object_store_support.zig").executeLakeCatalogAlloc(scratch, binding, options, context, .load);
            defer current.deinit(scratch);
            if (!std.mem.eql(u8, current.table.metadata_location, cut.metadata_location) or !std.mem.eql(u8, current.table.metadata_json, cut.metadata_json) or try @import("../serverless/lake_ingestion.zig").watermark(scratch, binding, options, context) != cut.pending.lsn) return error.SqlWriteConflict;
        }
    }

    fn rowPolicyProof(self: *Adapter, alloc: std.mem.Allocator, table: catalog.Table) !?[]const u8 {
        if (self.policy_proofs) |*proofs| if (proofs.get(table.id)) |cached| return cached;
        const scope = table.scope orelse return error.InvalidSqlBackendResponse;
        const identity: ?*const http_server.AuthenticatedIdentity = if (self.identity.*) |*value| value else null;
        const token_alloc = if (self.policy_proofs != null) self.server.alloc else alloc;
        const signed = try self.server.rowPolicyReadProof(
            token_alloc,
            identity,
            self.context,
            table.id,
            table.physical_name,
            scope.database,
            table.schema_version,
        );
        errdefer if (signed) |token| token_alloc.free(token);
        // Publication can change while minting the proof. Reject the new
        // generation before an existing binding opens a source with it.
        if (self.read_guard != null) try self.validateReadFence(true);
        if (self.policy_proofs) |*proofs| {
            try proofs.put(self.server.alloc, table.id, signed);
            return signed;
        }
        return signed;
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
                    .page_bytes = 256 * 1024,
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
        if (self.read_guard != null and self.runnable != null) scan_request.opts.execution_wait = .{ .ptr = self, .begin = spillWaitBegin, .end = spillWaitEnd };
        scan_request.opts.row_policy_principal_proof = (try self.rowPolicyProof(alloc, table)) orelse "";
        scan_request.opts.row_policy_database = (table.scope orelse return error.InvalidSqlBackendResponse).database;
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

    fn lakeLifetime(raw: *anyopaque) !void {
        var admission: ReadAdmission = .{ .adapter = @ptrCast(@alignCast(raw)) };
        try admission.checkLifetime();
    }
    fn lakeReadContext(self: *Adapter, deadline: ?u64) !@import("antfly_local_sources").serverless_query_lake_read_context.Context {
        const normalized = try self.context.platformDeadline();
        const original = normalized.deadline_ns;
        return .{
            .io = self.server.embedding_provider_runtime.io,
            .deadline_ns = if (deadline) |limit| @min(limit, original orelse limit) else original,
            .cancellation = @import("antfly_local_sources").storage_object_storage.CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn),
            .additional_checkpoint = .{ .ptr = self, .check = checkpoint },
            .authority = .{ .ptr = self, .check = lakeLifetime },
            .wait = if (self.read_guard != null and self.runnable != null) .{ .ptr = self, .begin = spillWaitBegin, .end = spillWaitEnd } else null,
        };
    }

    fn prepareRemoteRead(self: *Adapter) !void {
        if (self.read_remote_setup) |setup| try setup.prepare(setup.ptr);
    }

    const ReadAdmission = struct {
        adapter: *Adapter,
        pub fn remoteExecutor(self: *@This()) !?@import("antfly_local_sources").common_http_http_common.RequestExecutor {
            try self.adapter.prepareRemoteRead();
            return self.adapter.read_remote_executor;
        }
        pub fn retirementBinding(self: *@This()) !?@import("retained_read_retirement.zig").Binding {
            try self.adapter.prepareRemoteRead();
            return self.adapter.read_retirement;
        }
        pub fn checkpoint(self: *@This()) !void {
            try Adapter.checkpoint(self.adapter);
        }
        pub fn checkLifetime(self: *@This()) !void {
            if (try self.adapter.activeCleanup()) |borrow| return borrow.checkLifetime();
            try self.adapter.context.ensureActive();
            if (self.adapter.runnable) |execution| try execution.checkLifetime() else if (self.adapter.write_execution) |execution| try execution.checkLifetime();
        }
        pub fn deadlineNs(self: *@This()) !?u64 {
            // Protected cleanup has independent finite execution authority.
            if (try self.adapter.activeCleanup() != null) return null;
            return (try self.adapter.context.platformDeadline()).deadline_ns;
        }
        pub fn executionBoundary(self: *@This()) ?@import("antfly_local_sources").common_execution_wait.Boundary {
            if (self.adapter.read_guard == null or self.adapter.runnable == null) return null;
            return .{ .ptr = self.adapter, .begin = spillWaitBegin, .end = spillWaitEnd };
        }
        pub fn waitForProgress(self: *@This()) !void {
            try self.checkpoint();
            if (self.adapter.context.fanout_io) |borrow| {
                var receiver = try borrow.receive();
                try self.sleep(receiver.io());
            } else try self.sleep(self.adapter.server.sharedApiIo() orelse return error.ExecutionWaitUnsupported);
            try self.checkpoint();
        }
        fn sleep(self: *@This(), io: std.Io) !void {
            if (self.executionBoundary()) |boundary| try boundary.run(std.Io.sleep, .{ io, std.Io.Duration.fromMilliseconds(1), std.Io.Clock.awake }) else try io.sleep(.fromMilliseconds(1), .awake);
        }
    };

    const ReadCursor = struct {
        failed: bool = false,
        alloc: std.mem.Allocator,
        adapter: *Adapter,
        schema_version: u32,
        require_primary_digest: bool = false,
        projection: @import("antfly_local_sources").sql_document_row.Projection,
        view: @import("antfly_local_sources").api_table_read_source.RelationalReadView,
        column_page: ?catalog.Page = null,
        fn retireColumns(self: *@This()) void {
            if (self.column_page) |*page| page.deinit();
            self.column_page = null;
        }
        fn nextColumns(ptr: *anyopaque, _: std.mem.Allocator, limit: u32) !catalog.ColumnPage {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            // Mutation reads need per-row version/digest metadata, which the
            // native column-page interface does not carry.
            if (self.require_primary_digest) return error.UnsupportedSqlExecution;
            self.column_page = try next(ptr, @import("antfly_local_sources").sql_memory_budget.reclaimingAllocator(self.alloc), limit);
            errdefer self.retireColumns();
            const a = self.column_page.?.storage_page.?.arena.allocator();
            const Reader = struct {
                rows: []const catalog.Row,
                columns: []const catalog.Column,
                fn cell(raw: *anyopaque, _: std.mem.Allocator, row: usize, column: usize) !catalog.Row.Cell {
                    const view_: *@This() = @ptrCast(@alignCast(raw));
                    return view_.rows[row].cell(if (column == view_.columns.len) "_id" else view_.columns[column].name);
                }
                fn frozen(raw: *anyopaque, row: usize, column: usize) !?@import("antfly_local_sources").sql_execution_batch.FrozenJson {
                    const view_: *@This() = @ptrCast(@alignCast(raw));
                    if (column == view_.columns.len or view_.columns[column].type != .json) return null;
                    return view_.rows[row].frozenJson(view_.columns[column].name);
                }
                fn json(raw: *anyopaque, row: usize, column: usize) !?[]const u8 {
                    const view_: *@This() = @ptrCast(@alignCast(raw));
                    if (column == view_.columns.len or view_.columns[column].type != .json) return null;
                    return view_.rows[row].encodedJson(view_.columns[column].name);
                }
            };
            const reader = try a.create(Reader);
            reader.* = .{ .rows = self.column_page.?.rows, .columns = self.projection.columns };
            const values = try a.create(@import("antfly_local_sources").sql_execution_batch.Batch);
            values.* = .{ .reader = .{ .ptr = reader, .read = Reader.cell, .read_json = Reader.json, .read_frozen = Reader.frozen, .count = reader.rows.len, .width = reader.columns.len + 1 } };
            const names = try a.alloc([]const u8, values.width());
            for (self.projection.columns, names[0..self.projection.columns.len]) |column, *name| name.* = column.name;
            names[names.len - 1] = "_id";
            const selection = try a.alloc(usize, values.len());
            for (selection, 0..) |*position, i| position.* = i;
            return .{ .native = .{ .values = values, .names = names }, .selection = selection, .after = self.column_page.?.after };
        }

        fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
            const self: *ReadCursor = @ptrCast(@alignCast(ptr));
            self.retireColumns();
            if (self.failed) return error.RetainedReadRestartRequired;
            try checkpoint(self.adapter);
            errdefer self.failed = true;
            var budget: ReadAdmission = .{ .adapter = self.adapter };
            var bridge = @import("antfly_local_sources").common_read_admission.Adapter(*ReadAdmission){ .budget = &budget };
            const payload = bridge.payload();
            var page = try self.view.nextWithAdmission(alloc, limit, @import("antfly_local_sources").common_read_admission.Port.init(&payload));
            errdefer page.deinit();
            try checkpoint(self.adapter);
            if (page.rows.len > limit) return error.InvalidSqlBackendResponse;
            const rows = try page.arena.allocator().alloc(catalog.Row, page.rows.len);
            const layout = try self.projection.pageLayout(page.arena.allocator());
            for (page.rows, rows) |row, *out| {
                try checkpoint(self.adapter);
                if (row.schema_version != self.schema_version) return error.CatalogGenerationChanged;
                if (self.require_primary_digest and row.expected_content_digest == null) return error.InvalidSqlBackendResponse;
                out.* = try self.projection.adaptBorrowed(page.arena.allocator(), layout, row.sqlRow());
            }
            try checkpoint(self.adapter);
            return .{ .rows = rows, .after = page.after, .storage_page = page };
        }

        fn close(ptr: *anyopaque) void {
            const self: *ReadCursor = @ptrCast(@alignCast(ptr));
            self.retireColumns();
            self.view.deinit();
            self.projection.deinit(self.alloc);
            self.alloc.destroy(self);
        }
    };

    fn openScan(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        if (table.external_base_source != null) return try self.openLakeScan(alloc, table, request);
        // The admitted statement entry carries the original read port through
        // snapshot creation, signed remote dispatch and physical retirement.
        // The legacy single-read callback has no admission argument.
        const admitted_statement = self.runnable != null and (if (self.server.table_reads) |source| source.vtable.open_relational_statement_with_admission != null else false);
        if (self.range_reads != null or admitted_statement) {
            const statement = try openStatement(ptr, alloc, &.{.{ .table = table, .request = request }});
            errdefer statement.close(statement.ptr);
            const wrapper = try alloc.create(SingleStatementCursor);
            wrapper.* = .{ .alloc = alloc, .statement = statement };
            return wrapper.asCursor();
        }
        // Predicate-only entries and writes to unrelated physical tables do
        // not require merging this scan. Preserve native index selection
        // while fencing matching schema epochs even on read-only entries.
        // Guarded isolation above still owns its separate proof admission.
        if (self.staged != null and try @import("sql_session_overlay.zig").needsMerge(self.staged.?, table)) {
            const staged = self.staged.?;
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
        const native_cursor = try openNativeScan(ptr, alloc, table, request);
        // Empty overlays still need one retained statement view. A session
        // must never fall back to stateless pages from multiple visibility
        // cuts merely because it has not staged its first write yet.
        if (self.staged != null and native_cursor == null) return error.UnsupportedSqlExecution;
        return native_cursor;
    }

    const AggregateArtifactCursor = struct {
        a: std.mem.Allocator,
        adapter: *Adapter,
        table: catalog.Table,
        store: @import("lake_index_store.zig").Store,
        reader_lease: ?*@import("lake_index_reader_lease.zig").Handle = null,
        arena: std.heap.ArenaAllocator,
        artifacts: []const @import("antfly_local_sources").serverless_manifest_artifact_ref.ArtifactRef,
        recipe: @import("antfly_local_sources").sql_aggregate_materialization.Recipe,
        read_context: @import("antfly_local_sources").serverless_query_lake_read_context.Context,
        child: catalog.AggregatePartialCursor,
        execution: ?*@import("antfly_local_sources").common_request_admission.runnable_scheduler.Execution = null,
        fused: [256]bool = @splat(false),
        composition: ?*@import("lake_index_aggregate_composition.zig").Composition = null,
        transferred: bool = false,
        started: bool = false,
        final_request: ?catalog.FinalGroupRequest = null,
        final_page: std.heap.ArenaAllocator,
        final_rows: []const @import("antfly_local_sources").sql_operators.GroupResult = &.{},
        final_positions: []const usize = &.{},
        final_position: usize = 0,
        scan_quota: catalog.ScanQuota = .{ .capacity = 4096, .row_limit = std.math.maxInt(usize), .page_limit = std.math.maxInt(usize) },
        final_ordered: bool = false,
        fn canceled(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            self.adapter.context.ensureActive() catch return true;
            return false;
        }
        fn bindExecution(raw: *anyopaque, execution: *@import("antfly_local_sources").common_request_admission.runnable_scheduler.Execution) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.execution != null) return error.InvalidRunnableTransition;
            try self.read_context.withExecution(execution).ensureActive();
            self.execution = execution;
        }
        fn releaseExecution(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.execution = null;
        }
        fn next(raw: *anyopaque, a: std.mem.Allocator, maximum: u32) !?[]const @import("antfly_local_sources").sql_operators.GroupResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.started = true;
            try self.adapter.verify(a, self.table);
            const cursor = self.child;
            const result = cursor.nextFor(self.execution, a, maximum) catch |err| {
                try self.adapter.context.ensureActive();
                return err;
            };
            try self.adapter.context.ensureActive();
            return result;
        }
        fn closeCursor(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.transferred) close(raw);
        }
        fn adoptFinal(raw: *anyopaque, grouped: *@import("antfly_local_sources").sql_operators.Grouped, request: catalog.FinalGroupRequest) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.started or self.transferred) return error.InvalidSqlBackendResponse;
            const execution = if (self.adapter.cpuScope()) |scope| scope.execution else null;
            if (execution) |owner| try bindExecution(raw, owner);
            defer if (execution != null) releaseExecution(raw);
            const first = if (self.composition) |composition| prime: {
                const cursor = composition.cursor();
                const capability = cursor.coordinator_execution;
                if (execution != null and capability != null) try capability.?.bind(cursor.ptr, execution.?);
                defer if (execution != null and capability != null) capability.?.release(cursor.ptr);
                break :prime try composition.prime();
            } else self.child;
            const capability = first.coordinator_execution;
            if (execution != null and capability != null) try capability.?.bind(first.ptr, execution.?);
            defer if (execution != null and capability != null) capability.?.release(first.ptr);
            const proof = first.final_group_proof orelse return false;
            if (proof.slots.len != self.recipe.inputs.len or grouped.specs.len != self.recipe.inputs.len) return false;
            var covered: [256]bool = @splat(false);
            for (proof.slots) |slot| {
                if (slot >= self.recipe.inputs.len or covered[slot]) return error.InvalidSqlBackendResponse;
                covered[slot] = true;
            }
            for (grouped.specs, self.recipe.inputs) |spec, input| if (!std.meta.eql(spec, input.spec)) return error.InvalidSqlBackendResponse;
            try self.adapter.verify(self.a, self.table);
            if (request.point) |point| if (first.select_point) |select| try select(first.ptr, point);
            var selection = request.selection;
            selection.row_limit = request.row_limit;
            selection.page_limit = request.page_limit;
            selection.batch_rows = request.batch_rows;
            const ordered = if (first.select_keys) |select| try select(first.ptr, selection) else false;
            self.final_ordered = ordered;
            self.final_request = request;
            self.scan_quota = .{ .capacity = request.batch_rows, .row_limit = request.row_limit, .page_limit = request.page_limit };
            try grouped.adoptFinalized(.{ .ptr = self, .count = std.math.cast(usize, proof.count) orelse return error.SqlProgramLimitExceeded, .exact_count = request.predicate == null and request.point == null and !ordered, .ordered = ordered, .batch_rows = request.batch_rows, .next = nextFinal, .close = close }, request.page_limit);
            self.transferred = true;
            return true;
        }
        fn nextFinal(raw: *anyopaque, a: std.mem.Allocator, maximum: usize, exact: bool) anyerror!?@import("antfly_local_sources").sql_operators.GroupBatch {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const local = @import("antfly_local_sources");
            const execution = if (self.adapter.cpuScope()) |scope| scope.execution else null;
            if (execution) |owner| try bindExecution(raw, owner);
            defer if (execution != null) releaseExecution(raw);
            const request = self.final_request.?;
            // Retain one bounded provider page while small result pulls consume it.
            // Scan credit counts logical batches, independent of result framing.
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            while (self.final_position == self.final_positions.len) {
                _ = self.final_page.reset(.free_all);
                const pa = self.final_page.allocator();
                const rows = (try next(raw, pa, if ((self.final_ordered and request.selection.maximum_groups == null) or (request.selection.unordered_prefix and request.predicate == null and request.selection.maximum_groups != null)) @intCast(@min(maximum, request.batch_rows)) else request.batch_rows)) orelse return null;
                try self.scan_quota.admit(rows.len, false);
                const positions = try pa.alloc(usize, rows.len);
                var accepted: usize = 0;
                for (rows, 0..) |row, index| {
                    if (row.keys.len != self.recipe.keys.len or try local.sql_aggregate_partial.partialCount(row) != self.recipe.inputs.len) return error.InvalidSqlBackendResponse;
                    _ = scratch.reset(.free_all);
                    if (request.predicate) |predicate| if (!try predicate.matches(predicate.ptr, scratch.allocator(), row.keys)) continue;
                    positions[accepted] = index;
                    accepted += 1;
                }
                self.final_rows = rows;
                self.final_positions = positions[0..accepted];
                self.final_position = 0;
            }
            const count = @min(maximum, self.final_positions.len - self.final_position);
            const keys = try a.alloc([]const local.sql_scalar.Datum, count);
            const values = try a.alloc([]const local.sql_scalar.Datum, self.recipe.inputs.len);
            for (values) |*column| column.* = try a.alloc(local.sql_scalar.Datum, count);
            const ordinals = try a.alloc(u64, count);
            for (self.final_positions[self.final_position..][0..count], 0..) |index, output| {
                const row = self.final_rows[index];
                var populated: [256]bool = @splat(false);
                _ = scratch.reset(.free_all);
                for (0..values.len) |logical| {
                    const slot = if (row.aggregate_slots) |map| map[logical] else logical;
                    if (slot >= values.len or populated[slot]) return error.InvalidSqlBackendResponse;
                    populated[slot] = true;
                    @constCast(values[slot])[output] = if (exact) try local.sql_aggregate_partial.cell(a, try local.sql_aggregate_partial.read(scratch.allocator(), row, logical, self.recipe.inputs[slot].spec)) else try local.sql_aggregate_partial.finish(a, scratch.allocator(), row, logical, self.recipe.inputs[slot].spec);
                }
                const key_cells = try a.alloc(local.sql_scalar.Datum, row.keys.len);
                for (key_cells, row.keys) |*cell, input| cell.* = try local.sql_operators.cloneDatum(a, input);
                keys[output] = key_cells;
                ordinals[output] = row.ordinal;
            }
            self.final_position += count;
            return .{ .keys = .{ .rows = keys }, .aggregates = .{ .vectors = .{ .values = values, .count = count } }, .ordinals = ordinals };
        }

        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const cursor = self.child;
            cursor.close(cursor.ptr);
            self.store.deinit();
            self.destroyControl();
        }
        /// Both failed opens and completed cursors release dependent owners
        /// before freeing the descriptor that holds those owners.
        fn destroyControl(self: *@This()) void {
            const a = self.a;
            if (self.reader_lease) |lease| lease.deinit();
            self.final_page.deinit();
            self.arena.deinit();
            a.destroy(self);
        }
        fn openMemberFor(raw: *anyopaque, slot: usize, execution: *@import("antfly_local_sources").common_request_admission.runnable_scheduler.Execution) !catalog.AggregatePartialCursor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.execution != execution) return error.InvalidRunnableTransition;
            return openMember(raw, slot);
        }
        fn openMember(raw: *anyopaque, slot: usize) !catalog.AggregatePartialCursor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.adapter.verify(self.a, self.table);
            const recipe: @import("antfly_local_sources").sql_aggregate_materialization.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[slot .. slot + 1] };
            const reader = try @import("lake_index_aggregate_artifact.zig").Reader.openWithCacheFor(self.a, self.store.artifactStore(), self.artifacts[slot], recipe, .{ .ptr = self, .is_cancelled_fn = canceled }, .{ .cache = &self.adapter.server.lake_read_cache, .scope = self.store.identity, .context = self.read_context }, self.execution);
            errdefer reader.cursor().close(reader);
            if (self.recipe.inputs.len > 1) {
                try reader.setOutputSlot(@intCast(slot));
                self.fused[slot] = true;
                for (self.artifacts[slot + 1 ..], slot + 1..) |artifact, index| {
                    if (self.fused[index]) continue;
                    const input_recipe: @import("antfly_local_sources").sql_aggregate_materialization.Recipe = .{ .keys = self.recipe.keys, .inputs = self.recipe.inputs[index .. index + 1] };
                    const candidate = try @import("lake_index_aggregate_artifact.zig").Reader.openWithCacheFor(self.a, self.store.artifactStore(), artifact, input_recipe, .{ .ptr = self, .is_cancelled_fn = canceled }, .{ .cache = &self.adapter.server.lake_read_cache, .scope = self.store.identity, .context = self.read_context }, self.execution);
                    defer candidate.cursor().close(candidate);
                    if (try reader.fuseFor(candidate, @intCast(index), self.execution)) self.fused[index] = true;
                }
            }
            return reader.cursor();
        }
    };
    fn openAggregatePartials(raw: *anyopaque, a: std.mem.Allocator, table: catalog.Table, recipe: @import("antfly_local_sources").sql_aggregate_materialization.Recipe) !?catalog.AggregatePartialCursor {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        if (self.read_requirements.after_write != null or self.lake_visibility == .accepted or table.external_base_source == null or table.external_indexes == null) return null;
        const local = @import("antfly_local_sources");
        const artifact_api = @import("lake_index_aggregate_artifact.zig");
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const sa = scratch.allocator();
        var state = try local.metadata_lake_index_catalog.parse(sa, table.external_indexes.?.catalog_json);
        defer state.deinit();
        const publication = state.value.published orelse return null;
        if (recipe.inputs.len == 0 or recipe.inputs.len > 256) return null;
        const identities = try sa.alloc([]const u8, recipe.inputs.len);
        for (identities, 0..) |*identity, slot| identity.* = try @import("lake_index_native_aggregates.zig").recipeIdentity(sa, .{ .keys = recipe.keys, .inputs = recipe.inputs[slot .. slot + 1] });
        const eligible = for (identities) |identity| {
            const present = for (publication.declarations) |decl| {
                if (decl.artifact.kind == .algebraic_segment and artifact_api.supportsMetadataVersion(decl.artifact.metadata_version) and std.mem.eql(u8, decl.binding.index_config_hash, identity)) break true;
            } else false;
            if (!present) break false;
        } else true;
        if (!eligible and publication.directory == null) return null;
        try self.verify(sa, table);
        try self.checkLakeRead(sa, table);
        try self.server.prepareLakeCache();
        const normalized = try self.context.platformDeadline();
        var context: local.serverless_query_lake_read_context.Context = try self.lakeReadContext(normalized.deadline_ns);
        const options: @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions = .{ .node_config = self.server.cfg.node_config, .secret_store = self.server.cfg.secret_store };
        var source = try @import("lake_published_navigation.zig").openFor(a, self.server, table.id, if (table.external_indexes) |indexes| indexes.catalog_json else "", .{ .storage_mode = .relational, .external_base_source = table.external_base_source }, options.lakeOptions(), context, self.context, null, if (self.cpuScope()) |scope| scope.execution else null);
        defer source.deinit();
        const owner = try a.create(AggregateArtifactCursor);
        owner.a = a;
        owner.reader_lease = null;
        owner.arena = .init(a);
        owner.final_page = .init(a);
        var keep_owner = false;
        defer if (!keep_owner) owner.destroyControl();
        owner.store = @import("lake_index_store.zig").Store.openNativeFor(a, self.server.cfg.node_config, self.server.cfg.secret_store, true, self.server.cfg.deployment_mode, self.server.cfg.native_lake_artifact_base_dir, try self.lakeReadContext(null)) catch |err| {
            try context.ensureActive();
            if (err == error.OutOfMemory) return err;
            return null;
        };
        var keep_store = false;
        defer if (!keep_store) owner.store.deinit();
        if (self.server.source.lakeIndexLifecycleAuthority(self.context)) |authority| {
            owner.reader_lease = self.server.lake_reader_leases.acquire(self.server.embedding_provider_runtime.io, authority, table.id, publication.generation, context) catch |err| {
                try context.ensureActive();
                if (err == error.OutOfMemory or err == error.MetadataMutationOutcomeUnknown) return err;
                return null;
            };
            context = owner.reader_lease.?.readContext();
        }
        const opening_execution = if (self.cpuScope()) |scope| scope.execution else null;
        const selection_context = if (opening_execution) |execution| context.withExecution(execution) else context;
        var selected = (try @import("lake_index_selection.zig").selectCachedWithLease(sa, table, &source, &owner.store, selection_context, .automatic, &self.server.lake_read_cache, owner.reader_lease)) orelse return null;
        defer selected.deinit();
        const owned = owner.arena.allocator();
        const references = try owned.alloc(local.serverless_manifest_artifact_ref.ArtifactRef, identities.len);
        for (references, identities) |*reference, identity| {
            const declaration = for (selected.publication().declarations) |decl| {
                if (decl.artifact.kind == .algebraic_segment and artifact_api.supportsMetadataVersion(decl.artifact.metadata_version) and std.mem.eql(u8, decl.binding.index_config_hash, identity)) break decl;
            } else return null;
            reference.* = declaration.artifact;
            reference.name = try owned.dupe(u8, reference.name);
            reference.artifact_id = try owned.dupe(u8, reference.artifact_id);
            reference.checksum = try owned.dupe(u8, reference.checksum);
        }
        try self.verify(sa, table);
        // From this point errors propagate: selected immutable state cannot be
        // replaced with a fresh scan halfway through consumption.
        owner.fused = @splat(false);
        owner.composition = null;
        owner.transferred = false;
        owner.started = false;
        owner.final_request = null;
        owner.final_rows = &.{};
        owner.final_positions = &.{};
        owner.final_position = 0;
        owner.scan_quota = .{ .capacity = 4096, .row_limit = std.math.maxInt(usize), .page_limit = std.math.maxInt(usize) };
        owner.final_ordered = false;
        owner.a = a;
        owner.adapter = self;
        owner.table = table;
        owner.artifacts = references;
        owner.recipe = recipe;
        owner.read_context = context;
        owner.execution = opening_execution;
        defer owner.execution = null;
        owner.child = (if (recipe.inputs.len == 1) AggregateArtifactCursor.openMember(owner, 0) else composed: {
            const composition = try @import("lake_index_aggregate_composition.zig").Composition.create(a, recipe, .{ .ptr = owner, .open = AggregateArtifactCursor.openMember, .open_for = AggregateArtifactCursor.openMemberFor });
            owner.composition = composition;
            break :composed composition.cursor();
        }) catch |err| {
            try self.context.ensureActive();
            return err;
        };
        errdefer owner.child.close(owner.child.ptr);
        if (opening_execution) |execution| try execution.checkpoint();
        keep_store = true;
        keep_owner = true;
        return .{ .ptr = owner, .next = AggregateArtifactCursor.next, .close = AggregateArtifactCursor.closeCursor, .adopt_final_groups = AggregateArtifactCursor.adoptFinal, .coordinator_execution = .{ .bind = AggregateArtifactCursor.bindExecution, .release = AggregateArtifactCursor.releaseExecution } };
    }

    pub fn openLakeScan(self: *Adapter, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !catalog.Cursor {
        const statement = try self.openLakeStatement(alloc, &.{.{ .table = table, .request = request }});
        errdefer statement.close(statement.ptr);
        const owner = try alloc.create(SingleStatementCursor);
        owner.* = .{ .alloc = alloc, .statement = statement };
        return owner.asCursor();
    }

    fn checkLakeRead(self: *Adapter, alloc: std.mem.Allocator, table: catalog.Table) !void {
        // External snapshots cannot provide native serializable range proofs.
        if ((self.range_reads != null and !self.accepted_lake_repeatable) or self.dynamic_snapshot != null) return error.UnsupportedSqlExecution;
        if (try self.rowPolicyProof(alloc, table)) |proof| {
            if (self.policy_proofs == null) alloc.free(proof);
            return error.RowPolicyUnsupported;
        }
        if (try http_server.resolveEffectiveRowFilterJson(alloc, self.identity.*, table.physical_name)) |filter| {
            alloc.free(filter);
            return error.RowPolicyUnsupported;
        }
    }

    const LakeStatement = struct {
        alloc: std.mem.Allocator,
        cursors: []catalog.Cursor,
        sources: std.AutoHashMapUnmanaged(u64, *@import("antfly_local_sources").serverless_query_lake_serving.ServingSource) = .empty,
        opened: usize = 0,
        overlays: std.AutoHashMapUnmanaged(u64, *const @import("lake_sql_overlay.zig").Cut) = .empty,
        overlay_arena: ?std.heap.ArenaAllocator = null,
        native: ?catalog.StatementRead = null,
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            for (self.cursors[0..self.opened]) |cursor| cursor.close(cursor.ptr);
            if (self.native) |value| value.close(value.ptr);
            var values = self.sources.valueIterator();
            while (values.next()) |source| {
                source.*.deinit();
                self.alloc.destroy(source.*);
            }
            self.sources.deinit(self.alloc);
            self.overlays.deinit(self.alloc);
            if (self.overlay_arena) |*arena| arena.deinit();
            self.alloc.free(self.cursors);
            self.alloc.destroy(self);
        }
    };

    fn openLakeStatement(self: *Adapter, alloc: std.mem.Allocator, requests: []const catalog.StatementScan) !catalog.StatementRead {
        if (requests.len == 0 or requests.len > 64) return error.SqlProgramLimitExceeded;
        const owner = try alloc.create(LakeStatement);
        const cursors = alloc.alloc(catalog.Cursor, requests.len) catch |err| {
            alloc.destroy(owner);
            return err;
        };
        owner.* = .{ .alloc = alloc, .cursors = cursors };
        // owner owns cursors after allocation; avoid double free on failure.
        return self.fillLakeStatement(alloc, owner, requests) catch |err| {
            LakeStatement.close(owner);
            return err;
        };
    }

    fn loadSqlLakeCut(self: *Adapter, a: std.mem.Allocator, table: catalog.Table) !?@import("lake_sql_retained_cut.zig").Descriptor {
        const id = self.active_transaction orelse return null;
        var held = (try self.server.txn_sessions.getLakeCut(self.server.alloc, id, table.physical_name)) orelse return null;
        defer held.deinit(self.server.alloc);
        if (held.version != table.id or held.document_json == null) return error.CatalogGenerationChanged;
        const token = try std.json.parseFromSliceLeaky([]const u8, a, held.document_json.?, .{ .allocate = .alloc_always });
        var store = try @import("lake_index_store.zig").Store.openNativeFor(a, self.server.cfg.node_config, self.server.cfg.secret_store, false, self.server.cfg.deployment_mode, self.server.cfg.native_lake_artifact_base_dir, try self.lakeReadContext(null));
        defer store.deinit();
        var artifacts = store.artifactStore();
        const cut = try @import("lake_sql_retained_cut.zig").loadFor(a, &artifacts, try @import("native_retained_cut.zig").storeIdentity(a, store.locator), token, table, @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms, self.context.cancellation, try self.lakeReadContext(null));
        var normalized = try self.context.platformDeadline();
        const time = @import("antfly_platform").time;
        const wall_now = time.realtimeNs() / std.time.ns_per_ms;
        if (wall_now >= cut.expires_ms) return error.CatalogGenerationChanged;
        const expiry_deadline = time.monotonicNs() +| (cut.expires_ms - wall_now) * std.time.ns_per_ms;
        normalized.deadline_ns = @min(normalized.deadline_ns orelse expiry_deadline, expiry_deadline);
        self.context = normalized;
        const options: @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions = .{ .node_config = self.server.cfg.node_config, .secret_store = self.server.cfg.secret_store, .catalog_table_id = table.id, .catalog_generation = cut.object_generation };
        var current = try @import("../serverless/configured_object_store_support.zig").executeLakeCatalogAlloc(a, table.external_base_source.?.binding, options, try self.lakeReadContext(normalized.deadline_ns), .load);
        defer current.deinit(a);
        const root = try @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.metadata.parse(a, current.table.metadata_json);
        const metadata = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.metadata;
        if (!std.mem.eql(u8, try metadata.str(try metadata.get(root, "table-uuid")), cut.table_uuid)) return error.CatalogGenerationChanged;
        try Adapter.checkpoint(self);
        return cut;
    }
    fn saveSqlLakeCut(self: *Adapter, a: std.mem.Allocator, table: catalog.Table, cut: @import("lake_sql_retained_cut.zig").Descriptor, options: @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions, context: @import("antfly_local_sources").serverless_query_lake_read_context.Context) !void {
        const id = self.active_transaction orelse return error.SqlTransactionNotActive;
        var opened = try @import("../serverless/lake_ingestion.zig").openQueueFor(a, table.external_base_source.?.binding, options, context);
        defer opened.deinit();
        const pins = @import("../serverless/lake_snapshot_pins.zig");
        const pin: pins.Store = .{ .client = opened.client, .bucket = opened.bucket, .prefix = try pins.namespace(a, opened.prefix, table.external_base_source.?.binding, cut.table_uuid), .context = context };
        const now = @import("antfly_platform").time.realtimeNs();
        if (cut.expires_ms <= now / std.time.ns_per_ms) return error.CatalogGenerationChanged;
        _ = try pin.acquireFor(a, cut.snapshot_id, now, (cut.expires_ms - now / std.time.ns_per_ms) * std.time.ns_per_ms);
        var store = try @import("lake_index_store.zig").Store.openNativeFor(a, self.server.cfg.node_config, self.server.cfg.secret_store, false, self.server.cfg.deployment_mode, self.server.cfg.native_lake_artifact_base_dir, try self.lakeReadContext(null));
        defer store.deinit();
        var artifacts = store.artifactStore();
        const token = try @import("lake_sql_retained_cut.zig").save(a, &artifacts, try @import("native_retained_cut.zig").storeIdentity(a, store.locator), self.server.embedding_provider_runtime.io, cut, self.context.cancellation);
        const encoded = try std.json.Stringify.valueAlloc(a, token, .{});
        try Adapter.checkpoint(self);
        try self.server.txn_sessions.bindLakeCut(self.server.alloc, id, table.physical_name, table.id, encoded);
    }

    /// Wait before opening or persisting a new snapshot. A retained transaction
    /// cut is immutable and is checked separately without retrying execution.
    fn awaitSqlReceipt(self: *Adapter, a: std.mem.Allocator, polling_alloc: std.mem.Allocator, table: catalog.Table) !?@import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types.Table {
        const receipt = self.read_requirements.after_write orelse return null;
        const generation = if (table.external_indexes) |indexes| indexes.object_generation else 0;
        if (receipt.table_id != table.id) return null;
        if (receipt.object_generation != generation) return error.ReadReceiptGenerationMismatch;
        const binding = table.external_base_source.?.binding;
        if (binding.write_policy != .iceberg_writer) return error.UnsupportedReadVisibility;
        const options: @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions = .{ .node_config = self.server.cfg.node_config, .secret_store = self.server.cfg.secret_store, .catalog_table_id = table.id, .catalog_generation = generation };
        const normalized = try self.context.platformDeadline();
        const time = @import("antfly_platform").time;
        const wait_ms = self.read_requirements.wait_ms orelse 5000;
        const budget = self.shared_receipt_wait_budget orelse &self.receipt_wait_budget;
        const wait_deadline = budget.deadline(time.monotonicNs(), wait_ms);
        const deadline = @min(wait_deadline, normalized.deadline_ns orelse wait_deadline);
        // Zero permits one readiness check under the statement's I/O budget.
        // Positive budgets also bound catalog and WAL I/O, not just sleeps.
        const context: @import("antfly_local_sources").serverless_query_lake_read_context.Context = try self.lakeReadContext(if (wait_ms == 0) normalized.deadline_ns else deadline);
        var first = true;
        while (true) {
            try normalized.ensureActive();
            if (!first and time.monotonicNs() >= deadline) return error.ReadRequirementNotReady;
            first = false;
            context.ensureActive() catch |err| return receiptPollingError(normalized, deadline, err);
            {
                var attempt = std.heap.ArenaAllocator.init(polling_alloc);
                defer attempt.deinit();
                const scratch = attempt.allocator();
                var current = @import("../serverless/configured_object_store_support.zig").executeLakeCatalogAlloc(scratch, binding, options, context, .load) catch |err| return receiptPollingError(normalized, deadline, err);
                defer current.deinit(scratch);
                const coverage = try @import("../serverless/lake_ingestion.zig").coverage(scratch, current.table);
                const covered = if (self.lake_visibility == .accepted) (@import("../serverless/lake_ingestion.zig").pending(scratch, binding, options, context, coverage) catch |err| return receiptPollingError(normalized, deadline, err)).lsn else coverage;
                context.ensureActive() catch |err| return receiptPollingError(normalized, deadline, err);
                if (covered >= receipt.wal_lsn) return try std.json.parseFromSliceLeaky(@import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types.Table, a, try std.json.Stringify.valueAlloc(scratch, current.table, .{}), .{ .allocate = .alloc_always });
            }
            const now = time.monotonicNs();
            if (now >= deadline) return error.ReadRequirementNotReady;
            // Retire the polling attempt before lending CPU. Keep the original
            // receipt budget as an additional fence while reentering the queue.
            var wait: ReceiptPollWait = .{ .adapter = self, .deadline_ns = deadline };
            wait.sleep(context.io.?, .fromNanoseconds(@intCast(@min(deadline - now, 50 * std.time.ns_per_ms)))) catch |err|
                return receiptPollingError(normalized, deadline, err);
        }
    }

    const ReceiptPollWait = struct {
        adapter: *Adapter,
        deadline_ns: u64,

        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.adapter.context.ensureActive();
            if (@import("antfly_platform").time.monotonicNs() >= self.deadline_ns) return error.ReadRequirementNotReady;
        }

        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try check(self);
            try Adapter.spillWaitBegin(self.adapter);
        }

        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const execution = self.adapter.runnable orelse return error.InvalidRunnableTransition;
            const class = if (execution.entry) |*entry| entry.class else return error.InvalidRunnableTransition;
            // This restriction expires the receipt wait while queued without
            // replacing the operation, its deadline, or its retained charge.
            try execution.enterWithCheck(class, .{ .context = self, .check = check });
            try self.adapter.context.ensureActive();
            try self.adapter.validateReadFence(false);
        }

        fn sleep(self: *@This(), io: std.Io, duration: std.Io.Duration) !void {
            try check(self);
            try @import("antfly_local_sources").sql_execution_wait.run(
                if (self.adapter.runnable != null) .{ .ptr = self, .begin = begin, .end = end } else null,
                std.Io.sleep,
                .{ io, duration, std.Io.Clock.awake },
            );
        }
    };

    fn receiptPollingError(request: operation.RequestContext, deadline: u64, err: anyerror) anyerror {
        // Caller cancellation and statement expiration retain their SQLSTATE.
        request.ensureActive() catch |request_err| return request_err;
        if (@import("antfly_platform").time.monotonicNs() >= deadline) switch (err) {
            error.DeadlineExceeded, error.Canceled, error.Timeout => return error.ReadRequirementNotReady,
            else => {},
        };
        return err;
    }

    fn validateReadReceiptBinding(raw: *anyopaque, alloc: std.mem.Allocator, binding: @import("antfly_local_sources").sql_describe.BoundStatement) !void {
        const self: *Adapter = @ptrCast(@alignCast(raw));
        const receipt = self.read_requirements.after_write orelse return;
        if (binding.relation) |relation| {
            for (relation.scans) |scan_request| if (scan_request.table.id == receipt.table_id) return self.validateReceiptTable(alloc, scan_request.table);
        } else if (binding.table) |table| if (table.id == receipt.table_id) return self.validateReceiptTable(alloc, table);
        return error.ReadReceiptGenerationMismatch;
    }

    fn validateReceiptTable(self: *Adapter, alloc: std.mem.Allocator, table: catalog.Table) !void {
        const source = table.external_base_source orelse return error.UnsupportedReadVisibility;
        if (source.binding.write_policy != .iceberg_writer) return error.UnsupportedReadVisibility;
        const generation = if (table.external_indexes) |indexes| indexes.object_generation else 0;
        if (self.read_requirements.after_write.?.object_generation != generation) return error.ReadReceiptGenerationMismatch;
        // A zero-row plan can finish without opening any scan. Enforce the
        // receipt here too; actual scans still verify their selected read cut.
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const a = scratch.allocator();
        try self.checkLakeRead(a, table);
        if (self.lake_visibility == .accepted and self.active_transaction != null) {
            if (try self.loadSqlLakeCut(a, table)) |cut| {
                try self.read_requirements.requireCoverage(table.id, cut.object_generation, cut.pending.lsn, true);
                return;
            }
        }
        _ = try self.awaitSqlReceipt(a, alloc, table);
    }

    fn fillLakeStatement(self: *Adapter, alloc: std.mem.Allocator, owner: *LakeStatement, requests: []const catalog.StatementScan) !catalog.StatementRead {
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        var native: std.ArrayList(catalog.StatementScan) = .empty;
        // Pin every external table once; repeated aliases share the inventory.
        for (requests) |request| {
            if (request.table.external_base_source != null) {
                try self.verify(scratch.allocator(), request.table);
                try self.checkLakeRead(scratch.allocator(), request.table);
                try self.server.prepareLakeCache();
                if (owner.sources.contains(request.table.id)) continue;
                const source = try alloc.create(@import("antfly_local_sources").serverless_query_lake_serving.ServingSource);
                errdefer alloc.destroy(source);
                const schema: @import("antfly_local_sources").storage_schema.TableSchema = .{ .storage_mode = .relational, .external_base_source = request.table.external_base_source };
                var retained: ?@import("lake_sql_retained_cut.zig").Descriptor = null;
                if (self.lake_visibility == .accepted and self.active_transaction != null) {
                    if (owner.overlay_arena == null) owner.overlay_arena = .init(alloc);
                    retained = try self.loadSqlLakeCut(owner.overlay_arena.?.allocator(), request.table);
                }
                if (retained) |cut| if (self.read_requirements.after_write) |receipt| if (receipt.table_id == request.table.id) try self.read_requirements.requireCoverage(request.table.id, cut.object_generation, cut.pending.lsn, true);
                const ready_metadata = if (retained == null) try self.awaitSqlReceipt(scratch.allocator(), alloc, request.table) else null;
                const normalized = try self.context.platformDeadline();
                const lake_options: @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions = .{ .retained_catalog_metadata = if (retained) |cut| .{ .metadata_location = @constCast(cut.metadata_location), .metadata_json = @constCast(cut.metadata_json) } else ready_metadata, .node_config = self.server.cfg.node_config, .secret_store = self.server.cfg.secret_store, .catalog_table_id = request.table.id, .catalog_generation = if (request.table.external_indexes) |indexes| indexes.object_generation else 0 };
                source.* = try @import("lake_published_navigation.zig").openFor(alloc, self.server, request.table.id, if (request.table.external_indexes) |indexes| indexes.catalog_json else "", schema, lake_options.lakeOptions(), try self.lakeReadContext(normalized.deadline_ns), self.context, null, if (self.cpuScope()) |scope| scope.execution else null);
                errdefer source.deinit();
                try source.attachCache(&self.server.lake_read_cache, schema.external_base_source.?.binding, try self.lakeReadContext(normalized.deadline_ns));
                if (retained) |cut| {
                    if (!std.mem.eql(u8, source.inventory.snapshot_id, cut.snapshot_id)) return error.CatalogGenerationChanged;
                    const oa = owner.overlay_arena.?.allocator();
                    const overlay = try oa.create(@import("lake_sql_overlay.zig").Cut);
                    overlay.* = try @import("lake_sql_overlay.zig").Cut.initForTable(oa, cut.pending, request.table);
                    if (cut.pending.count() != 0) try owner.overlays.put(alloc, request.table.id, overlay);
                } else if (self.lake_visibility == .accepted and schema.external_base_source.?.binding.write_policy == .iceberg_writer) {
                    if (owner.overlay_arena == null) owner.overlay_arena = .init(alloc);
                    const oa = owner.overlay_arena.?.allocator();
                    const binding = schema.external_base_source.?.binding;
                    const context: @import("antfly_local_sources").serverless_query_lake_read_context.Context = try self.lakeReadContext(normalized.deadline_ns);
                    var current = try @import("../serverless/configured_object_store_support.zig").executeLakeCatalogAlloc(alloc, binding, lake_options, context, .load);
                    defer current.deinit(alloc);
                    const pinned_metadata = source.catalog_metadata orelse return error.InvalidLakeMetadata;
                    const metadata_uri = pinned_metadata.metadata_location;
                    const source_metadata = pinned_metadata.metadata_json;
                    const root = try @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.metadata.parse(oa, source_metadata);
                    const current_root = try @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.metadata.parse(oa, current.table.metadata_json);
                    const uuid = try @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.metadata.str(try @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.metadata.get(root, "table-uuid"));
                    if (!std.mem.eql(u8, uuid, try @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.metadata.str(try @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.metadata.get(current_root, "table-uuid")))) return error.CatalogGenerationChanged;
                    const cut = try @import("lake_search_overlay.zig").snapshotCoverage(root, source.inventory.snapshot_id);
                    const pending = try @import("../serverless/lake_ingestion.zig").pending(oa, binding, lake_options, context, cut);
                    if (self.read_requirements.after_write) |receipt| if (receipt.table_id == request.table.id) try self.read_requirements.requireCoverage(request.table.id, lake_options.catalog_generation, pending.lsn, false);
                    const overlay = try oa.create(@import("lake_sql_overlay.zig").Cut);
                    if (pending.count() != 0) try @import("lake_search_overlay.zig").requireNativeAncestry(current_root, source.inventory.snapshot_id);
                    // Persist the unmodified physical WAL before SQL converts
                    // timestamp values to its logical representation.
                    if (self.active_transaction != null) try self.saveSqlLakeCut(oa, request.table, .{
                        .name = if (request.table.scope) |scope| .{ .database = scope.database, .namespace = scope.namespace, .table = scope.name } else null,
                        .table_id = request.table.id,
                        .schema_version = request.table.schema_version,
                        .object_generation = if (request.table.external_indexes) |indexes| indexes.object_generation else 0,
                        .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms +| @import("lake_sql_retained_cut.zig").ttl_ms,
                        .metadata_location = metadata_uri,
                        .metadata_json = source_metadata,
                        .table_uuid = uuid,
                        .snapshot_id = source.inventory.snapshot_id,
                        .pending = pending,
                    }, lake_options, context);
                    overlay.* = try @import("lake_sql_overlay.zig").Cut.initForTable(oa, pending, request.table);
                    if (pending.count() != 0) try owner.overlays.put(alloc, request.table.id, overlay);
                }
                try owner.sources.put(alloc, request.table.id, source);
            } else try native.append(scratch.allocator(), request);
        }
        if (native.items.len != 0) owner.native = try openStatement(self, alloc, native.items);
        var native_index: usize = 0;
        for (requests, owner.cursors) |request, *cursor| {
            if (request.table.external_base_source != null) {
                cursor.* = if (owner.overlays.get(request.table.id)) |cut| overlay: {
                    const physical = try @import("lake_sql_overlay.zig").physicalRequest(owner.overlay_arena.?.allocator(), request.request, cut.*);
                    const child = (try @import("lake_index_sql_rows.zig").tryOpenAutoFor(alloc, self.server, request.table, physical, self.context, owner.sources.get(request.table.id).?, if (self.cpuScope()) |scope| scope.execution else null)) orelse try @import("lake_sql_cursor.zig").openPinned(alloc, request.table, physical, self.context, owner.sources.get(request.table.id).?);
                    errdefer child.close(child.ptr);
                    break :overlay try @import("lake_sql_overlay.zig").wrap(alloc, child, request.table, request.request, cut, self.context);
                } else if (request.request.index_range != null)
                    try @import("lake_index_sql_rows.zig").openPinnedFor(alloc, self.server, request.table, request.request, self.context, owner.sources.get(request.table.id).?, if (self.cpuScope()) |scope| scope.execution else null)
                else
                    (try @import("lake_index_sql_rows.zig").tryOpenAutoFor(alloc, self.server, request.table, request.request, self.context, owner.sources.get(request.table.id).?, if (self.cpuScope()) |scope| scope.execution else null)) orelse fallback: {
                        var physical_scan = request.request;
                        physical_scan.index_equality = null;
                        break :fallback try @import("lake_sql_cursor.zig").openPinned(alloc, request.table, physical_scan, self.context, owner.sources.get(request.table.id).?);
                    };
                errdefer cursor.close(cursor.ptr);
                if (!owner.overlays.contains(request.table.id)) cursor.* = try @import("lake_index_sql_rows.zig").wrapDynamic(alloc, self.server, request.table, request.request, self.context, owner.sources.get(request.table.id).?, cursor.*);
                try self.verify(scratch.allocator(), request.table);
            } else {
                cursor.* = owner.native.?.cursors[native_index];
                cursor.close = StatementRead.borrowedClose;
                native_index += 1;
            }
            owner.opened += 1;
        }
        return .{ .ptr = owner, .cursors = owner.cursors, .close = LakeStatement.close };
    }

    const SingleStatementCursor = struct {
        alloc: std.mem.Allocator,
        statement: catalog.StatementRead,
        fn asCursor(self: *@This()) catalog.Cursor {
            const child = self.statement.cursors[0];
            return .{
                .ptr = self,
                .next = next,
                .close = close,
                .next_columns = if (child.next_columns != null) nextColumns else null,
                .set_dynamic_filter = if (child.set_dynamic_filter != null) setDynamicFilter else null,
                .wants_exact_filter = if (child.wants_exact_filter != null) wantsExactFilter else null,
                .count_rows = if (child.count_rows != null) countRows else null,
                // Cursor's contract keeps children inside their parent's lifetime.
                // The statement therefore pins their shared source until close.
                .split_scan = if (child.split_scan != null) splitScan else null,
                .split_ordered = if (child.split_ordered != null) splitOrdered else null,
                .order_satisfied = child.order_satisfied,
                .column_statistics = child.column_statistics,
                .estimated_filtered_rows = child.estimated_filtered_rows,
                .estimated_rows = child.estimated_rows,
                .estimated_bytes = child.estimated_bytes,
                .ordered_split_bytes = child.ordered_split_bytes,
                .bind_execution = if (child.bind_execution != null) bindExecution else null,
                .coordinator_execution = if (child.coordinator_execution != null) .{ .bind = bindCoordinator, .release = releaseCoordinator } else null,
            };
        }
        fn bindCoordinator(raw: *anyopaque, execution: *http_server.RunnableScheduler.Execution) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const child = self.statement.cursors[0];
            try child.coordinator_execution.?.bind(child.ptr, execution);
        }
        fn releaseCoordinator(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const child = self.statement.cursors[0];
            child.coordinator_execution.?.release(child.ptr);
        }
        fn bindExecution(raw: *anyopaque, execution: *http_server.RunnableScheduler.Execution) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const child = self.statement.cursors[0];
            try child.bind_execution.?(child.ptr, execution);
        }
        fn countRows(raw: *anyopaque) !?u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const child = self.statement.cursors[0];
            return child.count_rows.?(child.ptr);
        }
        fn splitScan(raw: *anyopaque, alloc: std.mem.Allocator, maximum: usize) !?[]catalog.Cursor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const child = self.statement.cursors[0];
            return child.split_scan.?(child.ptr, alloc, maximum);
        }
        fn splitOrdered(raw: *anyopaque, alloc: std.mem.Allocator, maximum: usize) !?[]catalog.Cursor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const child = self.statement.cursors[0];
            return child.split_ordered.?(child.ptr, alloc, maximum);
        }
        fn next(raw: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const cursor = self.statement.cursors[0];
            return cursor.next(cursor.ptr, alloc, limit);
        }
        fn wantsExactFilter(raw: *anyopaque, columns: []const @import("antfly_local_sources").sql_dynamic_filter.Column) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const cursor = self.statement.cursors[0];
            return cursor.wants_exact_filter.?(cursor.ptr, columns);
        }
        fn setDynamicFilter(raw: *anyopaque, filter: *const @import("antfly_local_sources").sql_dynamic_filter.Filter) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const cursor = self.statement.cursors[0];
            return if (cursor.set_dynamic_filter) |install| try install(cursor.ptr, filter) else false;
        }
        fn nextColumns(raw: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.ColumnPage {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const cursor = self.statement.cursors[0];
            return (cursor.next_columns orelse return error.UnsupportedSqlExecution)(cursor.ptr, alloc, limit);
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
        const projection = try @import("antfly_local_sources").sql_document_row.Projection.init(alloc, table, request.fields);
        errdefer projection.deinit(alloc);
        const cursor = try alloc.create(ReadCursor);
        cursor.* = .{ .alloc = alloc, .adapter = self, .schema_version = table.schema_version, .require_primary_digest = request.include_primary_digest, .view = view, .projection = projection };
        return .{ .ptr = cursor, .next = ReadCursor.next, .next_columns = if (request.include_primary_digest) null else ReadCursor.nextColumns, .close = ReadCursor.close };
    }

    const StatementRead = struct {
        alloc: std.mem.Allocator,
        native: @import("antfly_local_sources").api_table_read_source.RelationalStatementRead,
        wrappers: []ReadCursor,
        cursors: []catalog.Cursor,
        overlays: bool,

        fn borrowedClose(_: *anyopaque) void {}

        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.overlays) for (self.cursors) |cursor| cursor.close(cursor.ptr);
            for (self.wrappers) |*wrapper| wrapper.retireColumns();
            self.native.deinit();
            for (self.wrappers) |*wrapper| {
                wrapper.retireColumns();
                wrapper.projection.deinit(self.alloc);
            }
            self.alloc.free(self.cursors);
            self.alloc.free(self.wrappers);
            self.alloc.destroy(self);
        }
    };

    const DynamicStatementRead = struct {
        alloc: std.mem.Allocator,
        views: []@import("antfly_local_sources").api_table_read_source.RelationalReadView,
        view_count: usize = 0,
        opened: usize = 0,
        wrappers: []ReadCursor,
        cursors: []catalog.Cursor,
        overlays: bool,

        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.overlays) for (self.cursors[0..self.opened]) |cursor| cursor.close(cursor.ptr);
            for (self.wrappers[0..self.opened]) |*wrapper| wrapper.retireColumns();
            for (self.views[0..self.view_count]) |view| view.deinit();
            for (self.wrappers) |*wrapper| {
                wrapper.retireColumns();
                wrapper.projection.deinit(self.alloc);
            }
            self.alloc.free(self.cursors);
            self.alloc.free(self.wrappers);
            self.alloc.free(self.views);
            self.alloc.destroy(self);
        }
    };

    fn captureDynamicStatement(self: *Adapter, alloc: std.mem.Allocator, reads: @import("antfly_local_sources").api_table_read_source.TableReadSource, table: []const u8, schema_version: u32) !@import("antfly_local_sources").api_table_read_source.RelationalStatementSnapshot {
        var budget: ReadAdmission = .{ .adapter = self };
        var bridge = @import("antfly_local_sources").common_read_admission.Adapter(*ReadAdmission){ .budget = &budget };
        const payload = bridge.payload();
        return reads.openRelationalStatementSnapshotWithAdmission(alloc, table, schema_version, .read_index, self.context.cancellation, (try self.context.platformDeadline()).deadline_ns, .init(&payload));
    }

    fn openDynamicStatement(self: *Adapter, alloc: std.mem.Allocator, requests: []const catalog.StatementScan) !catalog.StatementRead {
        const snapshot = self.dynamic_snapshot orelse return error.SqlStatementSnapshotRequired;
        const bound_table = self.dynamic_table orelse return error.SqlStatementSnapshotRequired;
        const observed = self.range_reads orelse return error.SqlRangeTrackingRequired;
        if (requests.len == 0 or requests.len > 64) return error.SqlProgramLimitExceeded;
        const retained = try alloc.create(DynamicStatementRead);
        errdefer alloc.destroy(retained);
        const views = try alloc.alloc(@import("antfly_local_sources").api_table_read_source.RelationalReadView, requests.len);
        errdefer alloc.free(views);
        const wrappers = try alloc.alloc(ReadCursor, requests.len);
        errdefer alloc.free(wrappers);
        var projected: usize = 0;
        errdefer for (wrappers[0..projected]) |wrapper| wrapper.projection.deinit(alloc);
        for (requests, wrappers) |request, *wrapper| {
            wrapper.projection = try @import("antfly_local_sources").sql_document_row.Projection.init(alloc, request.table, request.request.fields);
            projected += 1;
        }
        const cursors = try alloc.alloc(catalog.Cursor, requests.len);
        errdefer alloc.free(cursors);
        const overlay_staged = self.staged != null and self.staged.?.tables.len != 0;
        retained.* = .{ .alloc = alloc, .views = views, .wrappers = wrappers, .cursors = cursors, .overlays = overlay_staged };
        errdefer {
            if (overlay_staged) for (cursors[0..retained.opened]) |cursor| cursor.close(cursor.ptr);
            for (views[0..retained.view_count]) |view| view.deinit();
        }
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const temporary = scratch.allocator();
        var budget: ReadAdmission = .{ .adapter = self };
        var bridge = @import("antfly_local_sources").common_read_admission.Adapter(*ReadAdmission){ .budget = &budget };
        const payload = bridge.payload();
        const control = @import("antfly_local_sources").common_read_admission.Port.init(&payload);
        for (requests, views, wrappers, cursors) |request, *view, *wrapper, *cursor| {
            try self.context.ensureActive();
            if (!std.mem.eql(u8, request.table.physical_name, bound_table)) return error.SqlStatementSnapshotRequired;
            try self.verify(temporary, request.table);
            var query = request.request;
            // Dynamic retained cuts currently prove primary-key intervals.
            // Never let a planner's opportunistic auto-index selection turn
            // an owner-fenced read into an unproved secondary membership read.
            if (query.index_equality != null) return error.SqlStatementSnapshotRequired;
            query.primary_order = true;
            const scan_request = try self.prepareScan(temporary, request.table, query);
            const guarded = snapshot.openGuardedWithAdmission(alloc, .{ .table = bound_table, .from = scan_request.from, .to = scan_request.to, .opts = scan_request.opts }, control) catch |err| blk: {
                if (err != error.SqlRangeTrackingRequired or retained.view_count != 0 or observed.tables.len != 0) return err;
                // Activate the capability before any row or absence has been
                // observed, then replace the stale cut. Once a view exists we
                // must never silently mix two cuts within one SQL statement.
                const writes = self.server.table_writes orelse return err;
                try writes.activateRangeTracking(temporary, bound_table, self.context);
                const reads = self.server.table_reads orelse return error.SqlStatementSnapshotRequired;
                const replacement = try self.captureDynamicStatement(alloc, reads, bound_table, request.table.schema_version);
                snapshot.deinit();
                snapshot.* = replacement;
                break :blk try snapshot.openGuardedWithAdmission(alloc, .{ .table = bound_table, .from = scan_request.from, .to = scan_request.to, .opts = scan_request.opts }, control);
            };
            defer {
                for (guarded.owner_proofs) |owner| alloc.free(owner.proofs);
                alloc.free(guarded.owner_proofs);
            }
            view.* = guarded.view;
            retained.view_count += 1;
            if (self.staged) |staged| for (staged.tables) |old| {
                if (!std.mem.eql(u8, staged.physicalName(old.table_name), bound_table)) continue;
                if (old.schema_version != null and old.schema_version != request.table.schema_version) return error.CatalogGenerationChanged;
                if (old.range_guards) |guards| {
                    var checked = try @import("antfly_local_sources").api_range_read_guards.merge(temporary, guards.value, guarded.owner_proofs);
                    checked.deinit();
                }
            };
            const scope = request.table.scope orelse return error.InvalidSqlBackendResponse;
            const logical = try (system_catalog.Target{ .database = scope.database, .namespace = scope.namespace, .table = scope.name }).resourceNameAlloc(temporary);
            try observed.observeRanges(self.server.alloc, logical, bound_table, request.table.schema_version, guarded.owner_proofs);
            wrapper.* = .{ .alloc = alloc, .adapter = self, .schema_version = request.table.schema_version, .require_primary_digest = request.request.include_primary_digest, .view = view.*, .projection = wrapper.projection };
            cursor.* = .{ .ptr = wrapper, .next = ReadCursor.next, .next_columns = if (request.request.include_primary_digest) null else ReadCursor.nextColumns, .close = StatementRead.borrowedClose };
            if (if (overlay_staged) self.staged else null) |staged| {
                const row_filter = try http_server.resolveEffectiveRowFilterJson(temporary, self.identity.*, bound_table);
                cursor.* = try @import("sql_session_overlay.zig").open(alloc, cursor.*, staged, request.table, query, row_filter);
            }
            retained.opened += 1;
        }
        return .{ .ptr = retained, .cursors = cursors, .close = DynamicStatementRead.close };
    }

    fn estimateScans(ptr: *anyopaque, alloc: std.mem.Allocator, requests: []const catalog.StatementScan) ![]const catalog.ScanEstimate {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        try self.context.ensureActive();
        if (requests.len > 64) return error.SqlProgramLimitExceeded;
        const result = try alloc.alloc(catalog.ScanEstimate, requests.len);
        errdefer alloc.free(result);
        @memset(result, .{});
        if (requests.len == 0) return result;
        // Reuse the bounded optimizer planning generation. This is status
        // metadata, never a native statement read or a row/search operation.
        const generation = (try self.server.source.acquireJoinPlanning(.{ .clock = .{ .deadline_ns = self.context.deadline_ns, .io = self.context.deadline_io }, .cancellation = self.context.cancellation })) orelse return result;
        defer generation.release();
        for (requests, result) |request, *estimate| {
            try self.context.ensureActive();
            try self.verify(alloc, request.table);
            // Native status cannot price a lake inventory or a ranked search.
            if (request.table.external_base_source != null or request.request.search != null) continue;
            const table = generation.tables.get(request.table.physical_name) orelse continue;
            if (!table.identity_ready or table.table_id != request.table.id) continue;
            estimate.* = .{
                .estimated_rows = if (table.stats.row_count_known) table.stats.row_count else null,
                .estimated_bytes = if (table.stats.size_bytes_known) table.stats.size_bytes else null,
            };
        }
        return result;
    }

    fn openStatement(ptr: *anyopaque, alloc: std.mem.Allocator, requests: []const catalog.StatementScan) anyerror!catalog.StatementRead {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        for (requests) |request| if (request.table.external_base_source != null) return self.openLakeStatement(alloc, requests);
        if (self.dynamic_snapshot != null) return self.openDynamicStatement(alloc, requests);
        if (requests.len == 0 or requests.len > 64) return error.SqlProgramLimitExceeded;
        const read_source = self.server.table_reads orelse return error.TableNotFound;
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const scans = try temporary.alloc(@import("antfly_local_sources").api_table_read_source.RelationalStatementScan, requests.len);
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
        var projected: usize = 0;
        errdefer for (wrappers[0..projected]) |wrapper| wrapper.projection.deinit(alloc);
        for (requests, wrappers) |request, *wrapper| {
            wrapper.projection = try @import("antfly_local_sources").sql_document_row.Projection.init(alloc, request.table, request.request.fields);
            projected += 1;
        }
        const cursors = try alloc.alloc(catalog.Cursor, requests.len);
        errdefer alloc.free(cursors);
        var creation_budget: ReadAdmission = .{ .adapter = self };
        var creation_bridge = @import("antfly_local_sources").common_read_admission.Adapter(*ReadAdmission){ .budget = &creation_budget };
        const creation_payload = creation_bridge.payload();
        const creation_control = @import("antfly_local_sources").common_read_admission.Port.init(&creation_payload);
        const native = read_source.openRelationalStatementWithAdmission(alloc, scans, .read_index, creation_control) catch |err| blk: {
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
            break :blk try read_source.openRelationalStatementWithAdmission(alloc, scans, .read_index, creation_control);
        };
        errdefer native.deinit();
        if (native.views.len != requests.len) return error.InvalidSqlBackendResponse;
        var wrapped: usize = 0;
        errdefer if (overlay_staged) for (cursors[0..wrapped]) |cursor| cursor.close(cursor.ptr);
        for (requests, native.views, wrappers, cursors) |request, view, *wrapper, *cursor| {
            try self.verify(temporary, request.table);
            if (self.range_reads) |observed| {
                const proofs = try native.rangeProofsWithAdmission(temporary, wrapped, creation_control);
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
                        var checked = try @import("antfly_local_sources").api_range_read_guards.merge(temporary, guards.value, proofs);
                        checked.deinit();
                    }
                };
                const scope = request.table.scope orelse return error.InvalidSqlBackendResponse;
                const logical = try (system_catalog.Target{ .database = scope.database, .namespace = scope.namespace, .table = scope.name }).resourceNameAlloc(temporary);
                try observed.observeRanges(self.server.alloc, logical, request.table.physical_name, request.table.schema_version, proofs);
            }
            wrapper.* = .{ .alloc = alloc, .adapter = self, .schema_version = request.table.schema_version, .require_primary_digest = request.request.include_primary_digest, .view = view, .projection = wrapper.projection };
            cursor.* = .{ .ptr = wrapper, .next = ReadCursor.next, .next_columns = if (request.request.include_primary_digest) null else ReadCursor.nextColumns, .close = StatementRead.borrowedClose };
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
        // SQL execution requires retained typed pages. Public HTTP/NDJSON
        // scans have no statement snapshot or native-cell ownership contract;
        // do not silently choose that transport when a provider lacks SQL.
        _ = ptr;
        _ = alloc;
        if (table.storage_mode == .document or request.include_primary_digest) return error.UnsupportedSqlExecution;
        return error.SqlStatementSnapshotRequired;
    }

    fn prepareMutations(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) ![]const catalog.Mutation {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        try self.verify(alloc, table);
        const images = @import("antfly_local_sources").sql_mutation_images;
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
        var budget: ReadAdmission = .{ .adapter = self };
        var bridge = @import("antfly_local_sources").common_read_admission.Adapter(*ReadAdmission){ .budget = &budget };
        const payload = bridge.payload();
        const normalized = try view.normalizeWithAdmission(alloc, writes, @import("antfly_local_sources").common_read_admission.Port.init(&payload));
        const result = try images.merge(alloc, input, normalized);
        // RETURNING requires SELECT policy on the resulting row as well as
        // write authority. Do not publish postimages that an ordinary read hides.
        if (try http_server.resolveEffectiveRowFilterJson(alloc, self.identity.*, table.physical_name)) |json| {
            var filter = try @import("antfly_local_sources").search_pattern_filter.PreparedPatternFilter.init(alloc, json);
            defer filter.deinit();
            for (result) |mutation| if (mutation.row) |row| {
                if (!try filter.matchesJson(alloc, mutation.key, row)) return error.Forbidden;
            };
        }
        try self.verify(alloc, table);
        return result;
    }

    fn mutate(ptr: *anyopaque, alloc: std.mem.Allocator, scratch: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) !catalog.MutationOutcome {
        return mutateInternal(ptr, alloc, scratch, table, input, false);
    }

    fn mutatePrepared(ptr: *anyopaque, alloc: std.mem.Allocator, scratch: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) !catalog.MutationOutcome {
        return mutateInternal(ptr, alloc, scratch, table, input, true);
    }

    fn mutateInternal(ptr: *anyopaque, alloc: std.mem.Allocator, scratch: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation, already_prepared: bool) !catalog.MutationOutcome {
        const self: *Adapter = @ptrCast(@alignCast(ptr));
        try self.verify(alloc, table);
        const guards = try conflictGuards(alloc, input);
        if (self.active_transaction) |id| {
            const txn = @import("transactions.zig");
            var writes: std.ArrayList(db_types.BatchWrite) = .empty;
            var deletes: std.ArrayList([]const u8) = .empty;
            const predicates = try alloc.alloc(db_types.TransactionVersionPredicate, input.len);
            for (input, predicates) |mutation, *predicate| {
                predicate.* = .{ .key = mutation.key, .expected_version = mutation.expected_version, .expected_content_digest = mutation.expected_content_digest, .unique_absence = mutation.unique_absence };
                if (mutation.predicate_only) continue;
                if (self.inserting and mutation.expected_version == 0) if (self.staged) |staged| for (staged.tables) |existing| {
                    if (!std.mem.eql(u8, staged.physicalName(existing.table_name), table.physical_name)) continue;
                    for (existing.batch.writes) |write| if (std.mem.eql(u8, write.key, mutation.key)) return error.UniqueConstraintViolation;
                    for (existing.batch.deletes) |deleted| if (std.mem.eql(u8, deleted, mutation.key)) {
                        for (existing.predicates.items) |observed| if (std.mem.eql(u8, observed.key, mutation.key)) {
                            predicate.expected_version = observed.expected_version;
                            // Reinsert after a staged DELETE checks the original
                            // row version, rather than absence in durable storage.
                            if (observed.expected_version != 0) predicate.unique_absence = false;
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
                const scope = table.scope orelse return error.InvalidSqlBackendResponse;
                const policy_proof = try self.rowPolicyProof(alloc, table);
                const first_key = writes.items[0].key;
                const upper = try pointUpperBound(alloc, first_key);
                const view = (try source.openRelationalRead(alloc, table.physical_name, first_key, upper, .{
                    .limit = 1,
                    .inclusive_from = true,
                    .exclusive_to = true,
                    .relational_query = .{ .fields = &.{}, .schema_version = table.schema_version },
                    .row_policy_principal_proof = policy_proof orelse "",
                    .row_policy_database = scope.database,
                    .execution_deadline_ns = (try self.context.platformDeadline()).deadline_ns,
                    .cancellation = self.context.cancellation,
                }, .read_index)) orelse return error.UnsupportedSqlExecution;
                defer view.deinit();
                try self.verify(alloc, table);
                var budget: ReadAdmission = .{ .adapter = self };
                var bridge = @import("antfly_local_sources").common_read_admission.Adapter(*ReadAdmission){ .budget = &budget };
                const payload = bridge.payload();
                break :blk try view.normalizeWithAdmission(alloc, writes.items, @import("antfly_local_sources").common_read_admission.Port.init(&payload));
            } else writes.items;
            const scope = table.scope orelse return error.InvalidSqlBackendResponse;
            const logical = try (system_catalog.Target{ .database = scope.database, .namespace = scope.namespace, .table = scope.name }).resourceNameAlloc(alloc);
            var tables = [_]txn.TableCommitRequest{.{ .table_name = @constCast(logical), .schema_version = if (table.storage_mode == .document) table.schema_version else null, .relational_schema_version = if (table.storage_mode == .relational) table.schema_version else null, .batch = .{ .writes = normalized, .deletes = deletes.items }, .predicates = .{ .items = predicates, .capacity = predicates.len, .pointer_stability = .{} } }};
            defer if (tables[0].conflict_guards) |*owned| owned.deinit();
            if (guards) |value| try tables[0].mergeConflictGuards(alloc, value);
            var bindings = [_]txn.CatalogBinding{.{ .logical = logical, .physical = table.physical_name }};
            const statement = txn.OwnedTransactionCommitRequest{ .tables = &tables, .catalog_bindings = .{ .items = &bindings, .capacity = 1, .pointer_stability = .{} } };
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
            predicate.* = .{ .key = mutation.key, .expected_version = mutation.expected_version, .expected_content_digest = mutation.expected_content_digest, .unique_absence = mutation.unique_absence };
            if (mutation.predicate_only) continue;
            if (mutation.row) |row| {
                try writes.append(alloc, .{ .key = mutation.key, .value = try std.json.Stringify.valueAlloc(alloc, row, .{}), .json_null_fields = if (table.storage_mode == .document) &.{} else mutation.json_null_fields });
            } else try deletes.append(alloc, mutation.key);
        }
        var response = try self.completeNativeBatch(scratch, table.physical_name, .{ .schema_version = if (table.storage_mode == .document) table.schema_version else null, .relational_schema_version = if (table.storage_mode == .relational) table.schema_version else null, .writes = writes.items, .deletes = deletes.items, .predicates = predicates, .integrity_commands = if (guards) |value| value.commands else &.{}, .relational_integrity_generation_set = if (guards) |value| value.generation_set else null });
        defer response.deinit(scratch);
        self.outcome_transaction_id = mutationTransactionId(response.body);
        if (response.status == 200 or response.status == 201) return .committed;
        if (response.status == 202) return try committedMutationOutcome(response.body);
        // Preserve ambiguity: a transport/API failure after native admission
        // is never converted into a retryable, definitely-aborted SQL result.
        const failure = classifyMutationFailure(response.status, response.body);
        self.outcome_transaction_id = failure.transaction_id;
        return failure.err;
    }

    fn completeNativeBatch(self: *Adapter, alloc: std.mem.Allocator, table_name: []const u8, batch: db_types.BatchRequest) !@import("public_table_http.zig").OwnedResponse {
        try self.context.ensureActive();
        if (self.context.execution_team) |team| {
            if (team.state == .active) return error.InvalidRunnableTransition;
            // The durable executor is supplied by its owner across the kernel
            // boundary. An unavailable imported capability is authoritative;
            // never reconstruct its Threaded vtable from the raw runtime.
            const io = http_server.ApiHttpServer.configuredDurableIo(self.server.cfg) orelse return error.SqlWriteCapacityUnavailable;
            const runtime = @import("antfly_runtime_abi");
            const borrowed_io = runtime.native_abi.IoBorrow.init(&io);
            var completion = NativeBatchCompletion{ .adapter = self, .alloc = alloc, .table_name = table_name, .batch = batch };
            // Reserve the actual task and finite completion before the batch
            // handler can enter native storage. Admission errors remain definite
            // no-write outcomes, with no synchronous fallback.
            var task = try @import("runnable_completion_task.zig").Task.reserve(team.allocator, io, team.borrow, &.{
                .io = &borrowed_io,
                .deadline_ns = @as(u64, @intCast(@max(0, std.Io.Clock.now(.awake, io).nanoseconds))) +| 30 * std.time.ns_per_s,
            }, &completion, NativeBatchCompletion.run);
            defer task.deinit();
            try self.context.ensureActive();
            task.runAndJoin() catch |err| {
                // After entering the native handler, an allocation or task
                // failure cannot prove that no durable write was accepted.
                if (completion.entered) return error.SqlMutationOutcomeUnknown;
                return err;
            };
            // The handler also consumes thread-local commit identity/error
            // metadata in this child. Transfer its complete owned response only
            // after the physical join, while the prepared inputs, allocator,
            // request bridge, team and adapter remain alive.
            return completion.response orelse error.InvalidRunnableTransition;
        }
        return @import("public_table_http.zig").handleNativeTableBatch(alloc, table_name, batch, self.server.tableApi(self.context)) catch
            return error.SqlMutationOutcomeUnknown;
    }

    const NativeBatchCompletion = struct {
        adapter: *Adapter,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        batch: db_types.BatchRequest,
        entered: bool = false,
        response: ?@import("public_table_http.zig").OwnedResponse = null,

        fn run(raw: *anyopaque, _: @import("antfly_runtime_abi").runnable_abi.Borrowed, _: std.Io) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // Completion never replaces the original proposal deadline or
            // cancellation. Recheck that fence before native admission.
            try self.adapter.context.ensureActive();
            self.entered = true;
            self.response = try @import("public_table_http.zig").handleNativeTableBatch(self.alloc, self.table_name, self.batch, self.adapter.server.tableApi(self.adapter.context));
        }
    };

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

pub const diagnostic = @import("antfly_local_sources").sql_errors.describe;
pub const diagnosticMessage = @import("antfly_local_sources").sql_errors.message;

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
    if (status == 503) if (parsed.value.code) |code| {
        if (std.mem.eql(u8, code, "transaction_precommit_read_unavailable") and parsed.value.transaction_id == null and parsed.value.status == null)
            return .{ .err = error.SqlStatementReadUnavailable };
        if (std.mem.eql(u8, code, "constraint_activation_pending")) return .{ .err = error.SqlWriteCapacityUnavailable, .transaction_id = id };
    };
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
        if (std.mem.eql(u8, native, "SqlFeatureNotSupported")) return error.SqlFeatureNotSupported;
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

test "SQL read resume fences validate live policy and catalog before continuing" {
    const a = std.testing.allocator;
    const users_api = @import("../usermgr/mod.zig");
    const casbin = @import("antfly_casbin");
    const metadata = @import("../metadata/api.zig");
    const Case = enum { success, filter, permission, roles, roles_without_filter, metadata_without_filter, unprotected_roles_change, unprotected_metadata_change, account, replacement, schema, revision, physical, key_expiry, key_deletion, during_catalog, deadline, policy_enable, policy_generation, policy_disable, policy_pending, policy_transition };
    const Source = struct {
        const Stamp = @import("antfly_local_sources").system_catalog_policies.PublicationStamp;
        const schema_one =
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"additionalProperties":false}}}}
        ;
        const schema_two =
            \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"additionalProperties":false}}}}
        ;
        manager: *users_api.UserManager,
        calls: usize = 0,
        id: u64 = 7,
        revision: u64 = 19,
        physical: []const u8 = "private-7",
        schema: []const u8 = schema_one,
        mutate: bool = false,
        policy: ?Stamp = null,

        fn status(_: *anyopaque) !metadata.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn resolve(raw: *anyopaque, alloc: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (call == .policy_publication_status) {
                try std.testing.expect(context.row_policy_install_authority);
                return std.json.Stringify.valueAlloc(alloc, self.policy, .{});
            }
            if (call != .resolve_many) return error.TestUnexpectedCatalogCall;
            self.calls += 1;
            const request = call.resolve_many;
            try std.testing.expectEqual(@as(usize, 1), request.targets.len);
            try std.testing.expect(request.include_query_definitions);
            if (self.mutate) {
                self.mutate = false;
                try self.manager.setRowFilter("alice", "docs", "{\"term\":{\"tenant\":\"changed-during-catalog\"}}");
            }
            const tables = [_]?system_catalog.ResolvedTable{.{ .table_id = self.id, .name = self.physical, .query_definition = .{ .table_id = self.id, .schema_json = self.schema, .read_schema_json = "", .indexes_json = "{}" } }};
            return std.json.Stringify.valueAlloc(alloc, system_catalog.ResolvedMany{ .revision = self.revision, .tables = &tables }, .{});
        }
    };
    const Clock = struct {
        fn now() u64 {
            return value;
        }
        var value: u64 = 0;
    };
    for (std.enums.values(Case)) |case| {
        var store = users_api.MemoryStore.init(a);
        defer store.deinit();
        var policies = casbin.MemoryAdapter.init(a);
        defer policies.deinit();
        var users = try users_api.UserManager.init(a, store.iface(), try users_api.initDefaultEnforcer(a, policies.iface()));
        defer users.deinit();
        var read = try users_api.Permission.initOwned(a, .table, "*", .read);
        defer read.deinit(a);
        var user = try users.createUserWithMetadata("alice", "secret", &.{read}, "{\"tenant\":\"initial\"}");
        defer user.deinit(a);
        try users.addRoleToUser("alice", "role:reader");
        try users.setRowFilter("alice", "docs", "{\"conjuncts\":[{\"term\":{\"tenant\":{\"$auth\":\"metadata.tenant\"}}},{\"terms\":{\"roles\":{\"$auth\":\"roles\"}}}]}");
        if (case == .roles_without_filter or case == .metadata_without_filter or case == .unprotected_roles_change or case == .unprotected_metadata_change) try users.setRowFilter("alice", "docs", "null");
        var key: ?users_api.CreatedApiKey = null;
        defer if (key) |*value| value.deinit(a);
        const principal = if (case == .key_expiry or case == .key_deletion) blk: {
            key = try users.createApiKey("alice", "read", &.{read}, &.{}, null);
            break :blk try std.fmt.allocPrint(a, "api-key:{s}", .{key.?.key.key_id});
        } else try a.dupe(u8, "basic:alice");
        defer a.free(principal);
        var snapshot = try users.snapshotCredentialPolicies(principal);
        defer snapshot.deinit(a);
        var identity = try http_server.cloneCatalogIdentity(a, .{
            .user_instance_id = snapshot.instance_id,
            .username = snapshot.policy.username,
            .credential_principal = principal,
            .permissions = snapshot.policy.permissions,
            .row_filter = snapshot.policy.row_filter,
            .metadata_json = snapshot.policy.metadata_json,
            .roles = snapshot.policy.roles,
            .live_user_manager = &users,
        });
        defer identity.?.deinit(a);
        var source: Source = .{ .manager = &users };
        switch (case) {
            .policy_generation, .policy_disable, .policy_pending, .policy_transition, .roles_without_filter, .metadata_without_filter => source.policy = .{ .table_id = 7, .schema_version = 1, .generation = 2, .catalog_epoch = 3, .phase = .active },
            else => {},
        }
        var server = http_server.ApiHttpServer.init(a, .{ .auth_enabled = true, .user_manager = &users }, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.resolve, .supports_query_definitions = true } }, null, null);
        defer server.deinit();
        if (key) |created| {
            const raw = try std.fmt.allocPrint(a, "{s}:{s}", .{ created.key.key_id, created.key_secret });
            defer a.free(raw);
            const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(raw.len));
            defer a.free(encoded);
            _ = std.base64.standard.Encoder.encode(encoded, raw);
            const header = try std.fmt.allocPrint(a, "ApiKey {s}", .{encoded});
            defer a.free(header);
            const authenticated = try server.authenticateRequest(.{ .authorization = header });
            identity.?.deinit(a);
            identity = authenticated;
            try std.testing.expectEqual(user.instance_id, identity.?.user_instance_id.?);
        }
        var scheduler = try http_server.RunnableScheduler.Scheduler.init(.{ .slots = 2, .max_operations = 3, .max_bytes = 128, .control_bytes = 32, .checkpoint_budget = 1 });
        defer scheduler.close();
        var execution: http_server.RunnableScheduler.Execution = .{};
        defer execution.finish();
        Clock.value = 0;
        try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = std.time.ns_per_s, .retained_bytes = 17 }, .query);
        var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .runnable = &execution };
        try adapter.beginReadFence();
        defer adapter.endReadFence();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        _ = try Adapter.resolve(&adapter, arena.allocator(), .{ .table = "docs" }, .read);
        const captured_calls = source.calls;
        try adapter.validateReadFence(false);
        try std.testing.expectEqual(captured_calls, source.calls);
        try Adapter.spillWaitBegin(&adapter);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 17), scheduler.stats().retained_bytes);
        var other: http_server.RunnableScheduler.Execution = .{};
        defer other.finish();
        try other.start(&scheduler, .{ .io = std.testing.io }, .query);
        var control: http_server.RunnableScheduler.Execution = .{};
        defer control.finish();
        try control.start(&scheduler, .{ .io = std.testing.io }, .control);
        try std.testing.expectEqual(@as(usize, 2), scheduler.stats().runnable_slots);
        switch (case) {
            .success => {},
            .filter => try users.setRowFilter("alice", "docs", "{\"term\":{\"tenant\":\"changed\"}}"),
            .permission => try users.removePermissionFromUser("alice", "*", .table),
            .roles, .roles_without_filter, .unprotected_roles_change => try users.addRoleToUser("alice", "role:added"),
            .metadata_without_filter, .unprotected_metadata_change => {
                // Represent a replicated metadata update; this fixture owns
                // the manager exclusively and retains the old admitted copy.
                const updated = try a.dupe(u8, "{\"tenant\":\"replicated-update\"}");
                const metadata_slot = users.user_metadata.getPtr("alice").?;
                a.free(metadata_slot.*);
                metadata_slot.* = updated;
            },
            .account => {
                try users.deleteUser("alice");
                var replacement = try users.createUserWithMetadata("alice", "secret", &.{read}, "{\"tenant\":\"replacement\"}");
                defer replacement.deinit(a);
            },
            .replacement => source.id = 8,
            .schema => source.schema = Source.schema_two,
            .revision => source.revision += 1,
            .physical => source.physical = "renamed-7",
            .key_expiry => users.api_keys.getPtr(key.?.key.key_id).?.key.expires_at_ns = 0,
            .key_deletion => try users.deleteApiKey("alice", key.?.key.key_id),
            .during_catalog => source.mutate = true,
            .deadline => Clock.value = 2 * std.time.ns_per_s,
            .policy_enable => source.policy = .{ .table_id = 7, .schema_version = 1, .generation = 2, .catalog_epoch = 3, .phase = .active },
            .policy_generation => {
                source.policy.?.generation = 3;
                source.policy.?.catalog_epoch = 4;
            },
            .policy_disable => source.policy.?.phase = .disabled,
            .policy_pending, .policy_transition => {
                source.policy.?.generation = 3;
                source.policy.?.catalog_epoch = 4;
                source.policy.?.serving_generation = 2;
                source.policy.?.serving_catalog_epoch = 3;
                source.policy.?.phase = if (case == .policy_pending) .pending_install else .serving_install;
            },
        }
        other.finish();
        control.finish();
        switch (case) {
            .success, .policy_pending, .unprotected_roles_change, .unprotected_metadata_change => {
                try Adapter.spillWaitEnd(&adapter);
                try std.testing.expectEqual(captured_calls + 1, source.calls);
                try Adapter.checkpoint(&adapter);
                try std.testing.expectEqual(captured_calls + 2, source.calls);
                // Output is fenced even when there was no new runnable grant.
                try users.removePermissionFromUser("alice", "*", .table);
                try std.testing.expectError(error.Forbidden, adapter.validateReadFence(true));
            },
            .replacement, .schema, .revision, .physical => try std.testing.expectError(error.CatalogGenerationChanged, Adapter.spillWaitEnd(&adapter)),
            .deadline => try std.testing.expectError(error.DeadlineExceeded, Adapter.spillWaitEnd(&adapter)),
            .policy_enable, .policy_generation, .policy_disable, .policy_transition => try std.testing.expectError(error.RowPolicyCatalogChanged, Adapter.spillWaitEnd(&adapter)),
            else => try std.testing.expectError(error.Forbidden, Adapter.spillWaitEnd(&adapter)),
        }
        try std.testing.expectEqual(@as(?u64, std.time.ns_per_s), execution.entry.?.options.deadline_ns);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        std.debug.print("read fence case {s}: no runnable or metadata charges\n", .{@tagName(case)});
    }
}

test "SQL diagnostics preserve SQLSTATE and Unicode character positions" {
    try std.testing.expectEqualStrings("42601", sqlState(error.InvalidSqlSyntax));
    try std.testing.expectEqualStrings("0A000", sqlState(error.SqlFeatureNotSupported));
    try std.testing.expectEqual(error.SqlFeatureNotSupported, definiteMutationFailure(400, "SqlFeatureNotSupported").?);
    try std.testing.expectEqual(@as(u16, 501), httpStatus(error.UnsupportedSqlShape));
    try std.testing.expectEqual(@as(u16, 409), httpStatus(error.SqlMutationOutcomeUnknown));
    try std.testing.expectEqual(@as(?i64, 4), characterPosition("éé x", 5));
}

test "SQL transaction rejects policy definition and publication DDL before dispatch" {
    for ([_][]const u8{
        "CREATE POLICY visible ON accounts FOR SELECT USING (id > 0)",
        "ALTER POLICY visible ON accounts USING (id > 1)",
        "DROP POLICY visible ON accounts",
        "ALTER TABLE accounts ENABLE ROW LEVEL SECURITY",
        "ALTER TABLE accounts DISABLE ROW LEVEL SECURITY",
    }) |sql| {
        var compiled = try @import("antfly_local_sources").sql_compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.UnsupportedSqlExecution, rejectTransactionalDdl(compiled.statement));
    }
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
        fn resolve(ptr: *anyopaque, alloc: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            if (call == .policy_publication_status) {
                try std.testing.expect(context.row_policy_install_authority);
                const mode: *u8 = @ptrCast(@alignCast(ptr));
                if (mode.* == 0) return alloc.dupe(u8, "null");
                const policies = @import("antfly_local_sources").system_catalog_policies;
                const owner: policies.Publication.OwnerIdentity = .{ .group_id = 1, .descriptor_digest = std.mem.zeroes([32]u8) };
                const ack: policies.Publication.OwnerAck = .{ .owner = owner, .catalog_epoch = 1, .phase = if (mode.* == 3) .pending_disable else .pending_install, .applied_term = 1, .applied_index = 1, .bundle_digest = std.mem.zeroes([32]u8) };
                const serving_ack: policies.Publication.OwnerAck = .{ .owner = owner, .catalog_epoch = 1, .phase = if (mode.* == 3) .serving_disable else .serving_install, .applied_term = 1, .applied_index = 2, .bundle_digest = std.mem.zeroes([32]u8) };
                return std.json.Stringify.valueAlloc(alloc, policies.Publication{
                    .table_id = 7,
                    .schema_version = 9,
                    .schema_digest = std.mem.zeroes([32]u8),
                    .generation = 1,
                    .catalog_epoch = 1,
                    .phase = switch (mode.*) {
                        1 => .active,
                        2 => .pending_install,
                        3 => .disabled,
                        else => return error.TestUnexpectedPublicationMode,
                    },
                    .required_owners = &.{owner},
                    .acknowledged_owners = if (mode.* == 2) &.{} else &.{ack},
                    .serving_acknowledged_owners = if (mode.* == 2) &.{} else &.{serving_ack},
                }, .{});
            }
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
    try std.testing.expectEqualStrings("d", scan.opts.row_policy_database);
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
    adapter.range_reads = &guarded;
    fake = 1;
    try std.testing.expectError(error.RowPolicyAuthenticationRequired, adapter.prepareScan(alloc, table, request));
    fake = 2;
    try std.testing.expectError(error.RowPolicyCatalogChanged, adapter.prepareScan(alloc, table, request));
    fake = 3;
    const disabled = try adapter.prepareScan(alloc, table, request);
    try std.testing.expectEqualStrings("", disabled.opts.row_policy_principal_proof);
}

test "SQL API empty transaction overlays preserve native index planning" {
    const reads = @import("antfly_local_sources").api_table_read_source;
    const Fake = struct {
        auto_index: bool = true,
        opens: usize = 0,
        closes: usize = 0,
        fn resolve(_: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            if (call == .policy_publication_status) return alloc.dupe(u8, "null");
            return alloc.dupe(u8, "{\"revision\":3,\"tables\":[{\"table_id\":7,\"name\":\"physical\"}]}");
        }
        fn open(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, opts: db_types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.RelationalReadView {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(self.auto_index, opts.relational_query.?.auto_index);
            self.opens += 1;
            return .{ .ptr = ptr, .vtable = &.{ .next = unexpectedNext, .close = close } };
        }
        fn unexpectedNext(_: *anyopaque, _: std.mem.Allocator, _: u32) !reads.RelationalReadView.Page {
            return error.UnexpectedRead;
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closes += 1;
        }
    };
    var fake: Fake = .{};
    var server: http_server.ApiHttpServer = undefined;
    server.alloc = std.testing.allocator;
    server.source = .{ .ptr = &fake, .vtable = &.{ .status = undefined, .system_catalog = Fake.resolve } };
    server.table_reads = .{ .ptr = &fake, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .open_relational_read = Fake.open } };
    var identity: ?http_server.AuthenticatedIdentity = null;
    var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{} };
    const table: catalog.Table = .{ .id = 7, .physical_name = "physical", .schema_version = 9, .columns = &.{.{ .name = "id", .path = "id", .type = .string }}, .scope = .{ .database = "d", .namespace = "n", .name = "logical", .revision = 3 } };
    const request: catalog.Scan = .{ .fields = &.{"id"}, .limit = 8 };
    var staged: @import("transactions.zig").OwnedTransactionCommitRequest = .{};
    var staged_table: @import("transactions.zig").TableCommitRequest = .{ .table_name = @constCast("physical") };
    for (0..5) |step| {
        adapter.staged = if (step == 0) null else &staged;
        if (step >= 2) staged.tables = (&staged_table)[0..1];
        if (step == 3) {
            staged_table.table_name = @constCast("other");
            staged_table.batch.deletes = @constCast(&[_][]const u8{"a"});
        }
        if (step == 4) staged_table.table_name = @constCast("physical");
        fake.auto_index = step != 4;
        const cursor = (try Adapter.openScan(&adapter, std.testing.allocator, table, request)) orelse return error.MissingSqlCursor;
        cursor.close(cursor.ptr);
    }
    try std.testing.expectEqual(@as(usize, 5), fake.opens);
    try std.testing.expectEqual(fake.opens, fake.closes);
    staged_table.batch.deletes = &.{};
    staged_table.schema_version = 8;
    try std.testing.expectError(error.CatalogGenerationChanged, Adapter.openScan(&adapter, std.testing.allocator, table, request));
    try std.testing.expectEqual(@as(usize, 5), fake.opens);
    staged.tables = &.{};
    adapter.staged = &staged;
    server.table_reads.?.vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined };
    try std.testing.expectError(error.UnsupportedSqlExecution, Adapter.openScan(&adapter, std.testing.allocator, table, request));
    adapter.staged = null;
    try std.testing.expect((try Adapter.openScan(&adapter, std.testing.allocator, table, request)) == null);
}

test "SQL API document preparation uses native normalization and retains mutation fences" {
    const read_source = @import("antfly_local_sources").api_table_read_source;
    const View = read_source.RelationalReadView;
    const Fake = struct {
        normalized: bool = false,
        closed: bool = false,
        fn resolve(_: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            if (call == .policy_publication_status) return alloc.dupe(u8, "null");
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
    const reads = @import("antfly_local_sources").api_table_read_source;
    const contract = @import("antfly_local_sources").api_distributed_txn_contract;
    const metadata = @import("../metadata/api.zig");
    const compiler = @import("antfly_local_sources").sql_compiler;
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
        records: [1]@import("antfly_local_sources").common_topology_records.TableRecord = .{.{ .table_id = 3, .name = "physical", .schema_json = schema }},
        fn status(_: *anyopaque) !metadata.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn resolve(_: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            if (call == .write_validation) return std.json.Stringify.valueAlloc(alloc, .{ .schema_json = schema }, .{});
            if (call == .policy_publication_status) return alloc.dupe(u8, "null");
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
            const observations = try alloc.dupe(@import("antfly_local_sources").api_range_read_guards.Proof, &.{.{ .bucket = 98, .generation = self.generation }});
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
    const reads = @import("antfly_local_sources").api_table_read_source;
    const contract = @import("antfly_local_sources").api_distributed_txn_contract;
    const metadata = @import("../metadata/api.zig");
    const compiler = @import("antfly_local_sources").sql_compiler;
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
        records: [3]@import("antfly_local_sources").common_topology_records.TableRecord = .{
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
        fail_merge_proof: bool = false,
        expression_case: bool = false,
        conditional_case: bool = false,
        skip_case: bool = false,
        delete_case: bool = false,
        returning_case: bool = false,
        insert_returning_case: bool = false,
        expression_predicate_case: bool = false,
        grouped_predicate_case: bool = false,
        cte_case: bool = false,
        prepared_cte_insert_case: bool = false,
        prepared_cte_merge_case: bool = false,
        prepared_cte_batches: usize = 0,
        archive_id: u64 = 5,
        target_guard: bool = false,
        source_guard: bool = false,
        fn status(_: *anyopaque) !metadata.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn resolve(ptr: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (call == .write_validation) return std.json.Stringify.valueAlloc(alloc, .{ .schema_json = schema }, .{});
            if (call == .policy_publication_status) return alloc.dupe(u8, "null");
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
            try std.testing.expectEqual(@as(usize, if (self.prepared_cte_insert_case) 1 else 2), scans.len);
            self.captures += 1;
            for (scans, self.states[0..scans.len], self.views[0..scans.len]) |scan_request, *state, *view| {
                try std.testing.expectEqual(!self.prepared_cte_insert_case, scan_request.opts.include_range_proofs);
                const table_id: u64 = if (std.mem.eql(u8, scan_request.table, "physical_usage")) 3 else if (std.mem.eql(u8, scan_request.table, "physical_source")) 4 else if (std.mem.eql(u8, scan_request.table, "physical_archive")) 5 else return error.TestUnexpectedTable;
                state.* = .{ .table_id = table_id, .expression_case = self.expression_case, .conditional_case = self.conditional_case, .skip_case = self.skip_case, .delete_case = self.delete_case, .insert_returning_case = self.insert_returning_case, .expression_predicate_case = self.expression_predicate_case, .grouped_predicate_case = self.grouped_predicate_case, .cte_case = self.cte_case };
                view.* = .{ .ptr = state, .vtable = &.{ .next = next, .close = close } };
            }
            return .{ .ptr = self, .views = self.views[0..scans.len], .vtable = &.{ .close = close, .range_proofs = proofs } };
        }
        fn proofs(ptr: *anyopaque, alloc: std.mem.Allocator, index: usize) ![]reads.RelationalStatementRead.OwnerRangeProof {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const table_id = self.states[index].table_id;
            if ((table_id != 3 and self.fail_source_proof) or (table_id == 3 and self.prepared_cte_merge_case and self.fail_merge_proof)) return error.TestProofUnavailable;
            const group_id: u64 = if (table_id == 3) 5 else if (table_id == 4) 7 else 9;
            const range_id: u64 = if (table_id == 3) 6 else if (table_id == 4) 8 else 10;
            const observations = try alloc.dupe(@import("antfly_local_sources").api_range_read_guards.Proof, &.{.{ .bucket = @intCast(table_id), .generation = 1 }});
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
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings(if (self.prepared_cte_insert_case) "physical_archive" else "physical_usage", table);
            return .{ .ptr = ptr, .vtable = &.{ .next = undefined, .normalize = normalize, .close = close } };
        }
        fn normalize(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const db_types.BatchWrite) ![]db_types.BatchWrite {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, if (self.skip_case or self.delete_case) 0 else if (self.returning_case or self.insert_returning_case or self.cte_case or self.prepared_cte_insert_case or self.prepared_cte_merge_case) 1 else 2), writes.len);
            return alloc.dupe(db_types.BatchWrite, writes);
        }
        fn close(_: *anyopaque) void {}
        fn commitBatch(ptr: *anyopaque, alloc: std.mem.Allocator, tables: []const contract.TableCommitRequest, _: db_types.SyncLevel) !?contract.CommitOutcome {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expect(self.prepared_cte_insert_case);
            try std.testing.expectEqual(@as(usize, 1), tables.len);
            const table = tables[0];
            try std.testing.expectEqualStrings("physical_archive", table.table_name);
            try std.testing.expectEqual(@as(usize, 1), table.writes.len);
            try std.testing.expectEqual(@as(usize, 0), table.deletes.len);
            try std.testing.expect(self.states[0].done);
            const row = try std.json.parseFromSlice(std.json.Value, alloc, table.writes[0].value, .{});
            defer row.deinit();
            try std.testing.expectEqualStrings("a", row.value.object.get("id").?.string);
            self.prepared_cte_batches += 1;
            return .{ .committed = .{ .participant_count = 1 } };
        }
        fn commit(ptr: *anyopaque, alloc: std.mem.Allocator, _: db_types.TxnId, _: u64, tables: []const contract.TableCommitRequest, _: db_types.SyncLevel) !?contract.CommitOutcome {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.prepared_cte_merge_case) {
                try std.testing.expectEqual(@as(usize, 1), tables.len);
                const table = tables[0];
                try std.testing.expectEqualStrings("physical_usage", table.table_name);
                try std.testing.expect(table.range_guards.len != 0);
                for (table.range_guards) |guard| {
                    try std.testing.expectEqual(@as(u64, 3), guard.fence.table_id);
                    try std.testing.expectEqual(@as(u64, 3), guard.fence.route.identity_namespace.table_id);
                    try std.testing.expectEqual(@as(?u64, 1), guard.proofs[0].generation);
                }
                try std.testing.expectEqual(@as(usize, 1), table.writes.len);
                try std.testing.expectEqual(@as(usize, 1), table.predicates.len);
                try std.testing.expectEqualStrings("row-a", table.writes[0].key);
                try std.testing.expectEqual(@as(u64, 42), table.predicates[0].expected_version);
                const row = try std.json.parseFromSlice(std.json.Value, alloc, table.writes[0].value, .{});
                defer row.deinit();
                try std.testing.expectEqualStrings("old", row.value.object.get("status").?.string);
                self.commit_attempts += 1;
                if (self.source_conflict) return .{ .conflict = .{ .table_name = "physical_usage", .key = "row-a", .message = "self-read range changed", .retryable = true } };
                if (self.unknown_commit) return error.TestTransportFailure;
                self.commits += 1;
                return .{ .committed = .{ .participant_count = 1 } };
            }
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
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, @import("antfly_local_sources").sql_parity_fixtures.inventory, .{});
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
    var backend_runtime = try @import("antfly_local_sources").storage_background_runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer backend_runtime.deinit();
    var prepared_backend = @import("antfly_local_sources").storage_mem_backend.Backend.init(std.testing.allocator, .{});
    defer prepared_backend.close();
    var prepared_store = try prepared_backend.runtimeStore(std.testing.allocator, .{ .name = "merge-prepared-test" });
    defer prepared_store.deinit();
    var prepared_durable = @import("transactions.zig").DurableSessionStore.initRuntime(std.testing.allocator, &prepared_store);
    var server = http_server.ApiHttpServer.init(std.testing.allocator, .{ .backend_runtime = backend_runtime.ptr(), .session_store = &prepared_durable }, .{ .ptr = &fake, .vtable = &.{ .status = Fake.status, .system_catalog = Fake.resolve, .supports_query_definitions = true, .admin_snapshot = Fake.snapshot, .free_admin_snapshot = Fake.freeSnapshot } }, .{ .ptr = &fake, .supports_sql_range_guards = true, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .open_relational_statement = Fake.open, .open_relational_read = Fake.openNormalize } }, .{ .ptr = &fake, .supports_sql_range_guards = true, .vtable = &.{ .batch = undefined, .commit_batch = Fake.commitBatch, .commit_transaction_with_id = Fake.commit } });
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
    // sql-0005: the exact prepared CTE INSERT body reads usage_records while
    // writing archived_records through one target batch after the captured
    // read-committed source snapshot. PREPARE must not read or mutate.
    if (identity) |*owned| owned.deinit(std.testing.allocator);
    identity = null;
    identity = try http_server.cloneCatalogIdentity(std.testing.allocator, .{ .username = @constCast("merge_writer"), .permissions = &permissions });
    fake.prepared_cte_insert_case = true;
    const original_cte_insert = for (parsed.value.object.get("entries").?.array.items) |entry| {
        if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-0005")) break entry.object.get("sql").?.string;
    } else return error.TestMissingCorpusCase;
    const separator = std.mem.indexOf(u8, original_cte_insert, " AS ") orelse return error.TestInvalidCorpusCase;
    const insert_statement = original_cte_insert[separator + " AS ".len ..];
    const insert_body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = insert_statement }, .{});
    defer std.testing.allocator.free(insert_body);
    const captures_before_insert = fake.captures;
    const batches_before_insert = fake.prepared_cte_batches;
    var insert_prepare_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql/prepared");
    defer insert_prepare_request.deinit();
    insert_prepare_request.body = insert_body;
    var insert_prepare_context = httpx.Context.init(std.testing.allocator, std.testing.io, &insert_prepare_request);
    defer insert_prepare_context.deinit();
    var insert_prepare_response = try handler.prepareSQL(&insert_prepare_context);
    defer insert_prepare_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), insert_prepare_response.status.code);
    try std.testing.expectEqual(captures_before_insert, fake.captures);
    try std.testing.expectEqual(batches_before_insert, fake.prepared_cte_batches);
    const insert_prepared = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, insert_prepare_response.body.?, .{});
    defer insert_prepared.deinit();
    const insert_prepared_id = insert_prepared.value.object.get("prepared_id").?.string;
    var insert_execute_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql/prepared/execute");
    defer insert_execute_request.deinit();
    insert_execute_request.body = "{}";
    var insert_execute_context = httpx.Context.init(std.testing.allocator, std.testing.io, &insert_execute_request);
    defer insert_execute_context.deinit();
    var insert_execute_response = try handler.executePreparedSQL(&insert_execute_context, insert_prepared_id);
    defer insert_execute_response.deinit();
    if (insert_execute_response.status.code != 200) std.debug.print("sql-0005: {s}\n", .{insert_execute_response.body orelse ""});
    try std.testing.expectEqual(@as(u16, 200), insert_execute_response.status.code);
    const insert_result = try std.json.parseFromSlice(wire.SQLResponse, std.testing.allocator, insert_execute_response.body.?, .{});
    defer insert_result.deinit();
    try std.testing.expectEqualStrings("INSERT", insert_result.value.command_tag);
    try std.testing.expectEqual(@as(i64, 1), insert_result.value.rows_affected);
    try std.testing.expectEqual(captures_before_insert + 1, fake.captures);
    try std.testing.expectEqual(batches_before_insert + 1, fake.prepared_cte_batches);
    // sql-0008: the original CTE-backed self-MERGE must carry the source and
    // target range decision into one guarded native commit. This mounted API
    // fixture uses an owner-proof fake; the actual owner validation remains a
    // separate release gate.
    fake.prepared_cte_insert_case = false;
    fake.prepared_cte_merge_case = true;
    const original_cte_merge = for (parsed.value.object.get("entries").?.array.items) |entry| {
        if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-0008")) break entry.object.get("sql").?.string;
    } else return error.TestMissingCorpusCase;
    const merge_separator = std.mem.indexOf(u8, original_cte_merge, " AS ") orelse return error.TestInvalidCorpusCase;
    const merge_body = try std.json.Stringify.valueAlloc(std.testing.allocator, .{ .statement = original_cte_merge[merge_separator + " AS ".len ..] }, .{});
    defer std.testing.allocator.free(merge_body);
    const captures_before_merge = fake.captures;
    const attempts_before_merge = fake.commit_attempts;
    var merge_prepare_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql/prepared");
    defer merge_prepare_request.deinit();
    merge_prepare_request.body = merge_body;
    var merge_prepare_context = httpx.Context.init(std.testing.allocator, std.testing.io, &merge_prepare_request);
    defer merge_prepare_context.deinit();
    var merge_prepare_response = try handler.prepareSQL(&merge_prepare_context);
    defer merge_prepare_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), merge_prepare_response.status.code);
    try std.testing.expectEqual(captures_before_merge, fake.captures);
    try std.testing.expectEqual(attempts_before_merge, fake.commit_attempts);
    const merge_prepared = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, merge_prepare_response.body.?, .{});
    defer merge_prepared.deinit();
    const merge_prepared_id = merge_prepared.value.object.get("prepared_id").?.string;
    var merge_execute_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql/prepared/execute");
    defer merge_execute_request.deinit();
    merge_execute_request.body = "{}";
    var merge_execute_context = httpx.Context.init(std.testing.allocator, std.testing.io, &merge_execute_request);
    defer merge_execute_context.deinit();
    var merge_execute_response = try handler.executePreparedSQL(&merge_execute_context, merge_prepared_id);
    defer merge_execute_response.deinit();
    if (merge_execute_response.status.code != 200) std.debug.print("sql-0008: {s}\n", .{merge_execute_response.body orelse ""});
    try std.testing.expectEqual(@as(u16, 200), merge_execute_response.status.code);
    const merge_result = try std.json.parseFromSlice(wire.SQLResponse, std.testing.allocator, merge_execute_response.body.?, .{});
    defer merge_result.deinit();
    try std.testing.expectEqualStrings("MERGE", merge_result.value.command_tag);
    try std.testing.expectEqual(@as(i64, 1), merge_result.value.rows_affected);
    try std.testing.expectEqual(captures_before_merge + 1, fake.captures);
    try std.testing.expectEqual(attempts_before_merge + 1, fake.commit_attempts);
    for ([_]struct { conflict: bool, code: []const u8, retryable: bool }{
        .{ .conflict = true, .code = "40001", .retryable = true },
        .{ .conflict = false, .code = "40003", .retryable = false },
    }) |fault| {
        fake.source_conflict = fault.conflict;
        fake.unknown_commit = !fault.conflict;
        const captures_before_fault = fake.captures;
        const attempts_before_fault = fake.commit_attempts;
        const commits_before_fault = fake.commits;
        var fault_request = try httpx.Request.init(std.testing.allocator, .POST, "http://127.0.0.1/db/v1/sql/prepared/execute");
        defer fault_request.deinit();
        fault_request.body = "{}";
        var fault_context = httpx.Context.init(std.testing.allocator, std.testing.io, &fault_request);
        defer fault_context.deinit();
        var fault_response = try handler.executePreparedSQL(&fault_context, merge_prepared_id);
        defer fault_response.deinit();
        try std.testing.expectEqual(@as(u16, 409), fault_response.status.code);
        const fault_diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, std.testing.allocator, fault_response.body.?, .{});
        defer fault_diagnostic.deinit();
        try std.testing.expectEqualStrings(fault.code, fault_diagnostic.value.code);
        try std.testing.expectEqual(@as(?bool, fault.retryable), fault_diagnostic.value.retryable);
        try std.testing.expectEqual(captures_before_fault + 1, fake.captures);
        try std.testing.expectEqual(attempts_before_fault + 1, fake.commit_attempts);
        try std.testing.expectEqual(commits_before_fault, fake.commits);
    }
    fake.source_conflict = false;
    fake.unknown_commit = false;
    fake.fail_merge_proof = true;
    const attempts_before_proof_failure = fake.commit_attempts;
    var failed_proof = try compiler.compile(std.testing.allocator, original_cte_merge[merge_separator + " AS ".len ..], .{});
    defer failed_proof.deinit();
    try std.testing.expectError(error.TestProofUnavailable, adapter.execute(std.testing.allocator, &failed_proof, &.{}, .{}, null));
    try std.testing.expectEqual(attempts_before_proof_failure, fake.commit_attempts);
}

test "SQL stateless fallback rejects JSON scans before transport" {
    var identity: ?http_server.AuthenticatedIdentity = null;
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    var server: http_server.ApiHttpServer = undefined;
    // Backend construction borrows the executor. Catalog and transport remain
    // poisoned so rejected requests cannot touch those authorities.
    server.embedding_provider_runtime.io = io_impl.io();
    var adapter = Adapter{ .server = &server, .identity = &identity, .context = .{} };
    try std.testing.expectError(error.SqlStatementSnapshotRequired, Adapter.scan(&adapter, std.testing.allocator, .{ .id = 1, .physical_name = "rows", .schema_version = 1, .storage_mode = .relational, .columns = &.{} }, .{ .fields = &.{}, .limit = 1 }));
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
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    var server: http_server.ApiHttpServer = undefined;
    // Backend construction borrows the executor. Catalog and transport remain
    // poisoned so rejected requests cannot touch those authorities.
    server.embedding_provider_runtime.io = io_impl.io();
    var adapter = Adapter{ .server = &server, .identity = &identity, .context = .{} };
    const compiler = @import("antfly_local_sources").sql_compiler;
    const runtime = @import("antfly_local_sources").sql_runtime;
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
    try std.testing.expectEqual(error.SqlMutationOutcomeUnknown, classifyMutationFailure(503, "write unavailable").err);
    try std.testing.expectEqual(error.SqlStatementReadUnavailable, classifyMutationFailure(503, "{\"code\":\"transaction_precommit_read_unavailable\"}").err);
    inline for (.{
        "{\"code\":\"transaction_precommit_read_unavailable\",\"status\":\"committed_pending\"}",
        "{\"code\":\"transaction_precommit_read_unavailable\",\"transaction_id\":\"0123456789abcdef0123456789abcdef\"}",
        "{\"code\":\"transaction_precommit_read_unavailable\",\"transaction_id\":\"invalid\"}",
    }) |body| try std.testing.expectEqual(error.SqlMutationOutcomeUnknown, classifyMutationFailure(503, body).err);
    try std.testing.expectEqual(error.SqlMutationOutcomeUnknown, classifyMutationFailure(500, "{\"code\":\"transaction_precommit_read_unavailable\"}").err);
    try std.testing.expectEqual(error.SqlWriteCapacityUnavailable, classifyMutationFailure(503, "{\"code\":\"constraint_activation_pending\"}").err);
    try std.testing.expectEqual(error.SqlMutationOutcomeUnknown, classifyMutationFailure(503, "{\"code\":\"constraint_activation_pending\",\"status\":\"committed_pending\"}").err);
    try std.testing.expectEqual(error.SqlMutationOutcomeUnknown, classifyMutationFailure(503, "write committed locally; standby durability acknowledgment pending").err);
    try std.testing.expectEqual(catalog.MutationOutcome.committed_pending, try committedMutationOutcome("{\"status\":\"committed_pending\"}"));
    try std.testing.expectEqual(catalog.MutationOutcome.committed_repair_required, try committedMutationOutcome("{\"status\":\"committed_repair_required\"}"));
    try std.testing.expectEqual(catalog.MutationOutcome.committed_graph_metric_materialization_rejected, try committedMutationOutcome("{\"status\":\"committed_repair_required\",\"failure\":{\"code\":\"graph_metric_materialization_rejected\"}}"));
    try std.testing.expectError(error.SqlMutationOutcomeUnknown, committedMutationOutcome("{\"status\":\"unknown\"}"));
}

test "SQL direct conflict scalar uses one guarded native cut through owner and commit" {
    const alloc = std.testing.allocator;
    const reads = @import("antfly_local_sources").api_table_read_source;
    const contract = @import("antfly_local_sources").api_distributed_txn_contract;
    const metadata = @import("../metadata/api.zig");
    const compiler = @import("antfly_local_sources").sql_compiler;
    const integrity = @import("antfly_local_sources").api_relational_integrity_commit;
    const activation = @import("antfly_local_sources").storage_db_relational_integrity_activation_contract;
    const native_catalog = @import("antfly_local_sources").storage_db_relational_integrity_catalog;
    const View = reads.RelationalReadView;
    const Fake = struct {
        const Self = @This();
        const schema =
            \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","nullable":true},"quantity":{"type":"integer"},"amount":{"type":"integer"}},"additionalProperties":false}}}}
        ;
        const State = struct { point: bool = false, absent: bool = false, done: bool = false };
        envelope: []const u8,
        digest: [32]u8,
        generation_set: [32]u8,
        tuple: []const u8 = "",
        claim: bool = false,
        captures: usize = 0,
        point_reads: usize = 0,
        publication_reads: usize = 0,
        dynamic_opens: usize = 0,
        commits: usize = 0,
        expect_status: bool = false,
        phantom_on_commit: bool = false,
        absence_case: bool = false,
        views: [2]View = undefined,
        states: [2]State = undefined,
        records: [1]@import("antfly_local_sources").common_topology_records.TableRecord = .{.{ .table_id = 3, .name = "physical_usage", .schema_json = schema }},
        ranges: [1]@import("antfly_local_sources").common_topology_records.RangeRecord = .{.{ .group_id = 5, .table_id = 3, .start_key = "" }},
        fn status(_: *anyopaque) !metadata.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn resolve(ptr: *anyopaque, allocator: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            if (call == .write_validation) return std.json.Stringify.valueAlloc(allocator, .{ .schema_json = schema }, .{});
            if (call == .policy_publication_status) {
                try std.testing.expect(context.row_policy_install_authority);
                const self: *Self = @ptrCast(@alignCast(ptr));
                self.publication_reads += 1;
                return allocator.dupe(u8, "null");
            }
            if (call != .resolve_many) return error.TestUnexpectedCatalogCall;
            const request = call.resolve_many;
            if (request.storage_names.len != 0) {
                const names = try allocator.alloc(?[]const u8, request.storage_names.len);
                for (request.storage_names, names) |physical, *name| {
                    try std.testing.expectEqualStrings("physical_usage", physical);
                    name.* = try (system_catalog.Target{ .table = "usage_records" }).resourceNameAlloc(allocator);
                }
                return std.json.Stringify.valueAlloc(allocator, system_catalog.ResolvedMany{ .revision = 2, .tables = &.{}, .logical_names = names }, .{});
            }
            const tables = try allocator.alloc(?system_catalog.ResolvedTable, request.targets.len);
            for (request.targets, tables) |target, *table| {
                try std.testing.expectEqualStrings("usage_records", target.table);
                table.* = .{ .table_id = 3, .name = "physical_usage", .query_definition = if (request.include_query_definitions) .{ .table_id = 3, .schema_json = schema, .read_schema_json = "", .indexes_json = "{}" } else null };
            }
            return std.json.Stringify.valueAlloc(allocator, system_catalog.ResolvedMany{ .revision = 2, .tables = tables }, .{});
        }
        fn snapshot(ptr: *anyopaque) !metadata.AdminSnapshot {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return .{ .status = try status(ptr), .tables = &self.records, .ranges = &self.ranges, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn freeSnapshot(_: *anyopaque, _: *metadata.AdminSnapshot) void {}
        fn lookup(ptr: *anyopaque, allocator: std.mem.Allocator, _: []const u8, key: []const u8, opts: db_types.LookupOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (opts.relational_integrity_catalog) return .{ .json = try allocator.dupe(u8, self.envelope), .version = 0 };
            if (opts.relational_integrity_jobs_json.len != 0) {
                if (!self.claim) return null;
                const request = try std.json.parseFromSliceLeaky(struct { address: @import("antfly_local_sources").storage_db_relational_integrity_contract.Address }, allocator, opts.relational_integrity_jobs_json, .{ .ignore_unknown_fields = true });
                const owner: @import("antfly_local_sources").storage_db_relational_integrity_contract.Claim = .{ .tuple = self.tuple, .parent_table = "physical_usage", .parent_key = "stored-1", .schema_version = 1 };
                return .{ .json = try std.json.Stringify.valueAlloc(allocator, .{ .address = request.address, .claim = owner, .references = &[_]@import("antfly_local_sources").storage_db_relational_integrity_contract.Reference{} }, .{}), .version = 0 };
            }
            if (opts.relational_activation_json.len == 0) {
                if (std.mem.eql(u8, key, "stored-1")) return .{ .json = try allocator.dupe(u8, "{\"id\":\"u1\",\"status\":\"ready\",\"quantity\":8,\"amount\":5}"), .version = 9, .expected_content_digest = @splat(9) };
                return null;
            }
            try std.testing.expectEqualStrings("{\"mode\":\"status\"}", opts.relational_activation_json);
            const coverage = .{ .schema_version = @as(u32, 1), .schema_digest = self.digest, .generation_set = self.generation_set, .owner = @as([32]u8, @splat(1)), .range_start = @as([]const u8, ""), .range_end = @as([]const u8, ""), .unique_covered = true, .state = activation.State.validating, .phase = activation.Phase.foreign_key, .rows_scanned = @as(u64, 1), .failure = @as([]const u8, "") };
            try std.testing.expectEqualStrings("", key);
            return .{ .json = try std.json.Stringify.valueAlloc(allocator, coverage, .{}), .version = 0 };
        }
        fn open(ptr: *anyopaque, _: std.mem.Allocator, scans: []const reads.RelationalStatementScan, _: @import("../raft/read_gate.zig").ReadConsistency) !reads.RelationalStatementRead {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expect(scans.len <= self.views.len);
            self.captures += 1;
            for (scans, self.states[0..scans.len], self.views[0..scans.len]) |scan, *state, *view| {
                try std.testing.expectEqualStrings("physical_usage", scan.table);
                state.* = .{ .point = std.mem.eql(u8, scan.from, "stored-1") };
                if (state.point) self.point_reads += 1;
                view.* = .{ .ptr = state, .vtable = &.{ .next = next, .close = close } };
            }
            return .{ .ptr = self, .views = self.views[0..scans.len], .vtable = &.{ .close = close, .range_proofs = proofs } };
        }
        fn openSnapshot(ptr: *anyopaque, _: std.mem.Allocator, table: []const u8, _: u32, _: @import("../raft/read_gate.zig").ReadConsistency, _: ?@import("antfly_cancellation").CancellationToken, _: ?u64) !reads.RelationalStatementSnapshot {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("physical_usage", table);
            self.captures += 1;
            return .{ .ptr = self, .vtable = &.{ .open = openSnapshotView, .open_guarded = openSnapshotGuarded, .close = close } };
        }
        fn openSnapshotView(ptr: *anyopaque, _: std.mem.Allocator, scan: reads.RelationalStatementScan) !View {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("physical_usage", scan.table);
            try std.testing.expect(scan.opts.include_range_proofs);
            self.dynamic_opens += 1;
            const state = &self.states[0];
            state.* = .{ .point = std.mem.eql(u8, scan.from, "stored-1"), .absent = self.absence_case and !std.mem.eql(u8, scan.from, "stored-1") };
            if (state.point) self.point_reads += 1;
            return .{ .ptr = state, .vtable = &.{ .next = next, .close = close } };
        }
        fn openSnapshotGuarded(ptr: *anyopaque, allocator: std.mem.Allocator, scan: reads.RelationalStatementScan) !reads.RelationalStatementSnapshot.GuardedRead {
            const view = try openSnapshotView(ptr, allocator, scan);
            return .{ .view = view, .owner_proofs = try proofs(ptr, allocator, 0) };
        }
        fn proofs(ptr: *anyopaque, allocator: std.mem.Allocator, index: usize) ![]reads.RelationalStatementRead.OwnerRangeProof {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const observations = try allocator.dupe(@import("antfly_local_sources").api_range_read_guards.Proof, &.{.{ .bucket = if (self.states[index].absent) 99 else 98, .generation = 1 }});
            return allocator.dupe(reads.RelationalStatementRead.OwnerRangeProof, &.{.{ .fence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 2, .table_id = 3, .topology_epoch = 4, .route = .{ .group_id = 5, .range_id = 6, .identity_namespace = .{ .table_id = 3, .shard_id = 5, .range_id = 6 } } }, .proofs = observations }});
        }
        fn next(ptr: *anyopaque, allocator: std.mem.Allocator, _: u32) !View.Page {
            const state: *State = @ptrCast(@alignCast(ptr));
            var arena = std.heap.ArenaAllocator.init(allocator);
            errdefer arena.deinit();
            if (state.done or state.absent) return .{ .arena = arena, .rows = &.{}, .after = null };
            state.done = true;
            const rows = try arena.allocator().alloc(View.Row, 1);
            rows[0] = .{ .id = "stored-1", .version = 9, .schema_version = 1, .value = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"id\":\"u1\",\"status\":\"ready\",\"quantity\":8,\"amount\":5}", .{}), .expected_content_digest = @splat(9) };
            return .{ .arena = arena, .rows = rows, .after = null };
        }
        fn normalize(_: *anyopaque, allocator: std.mem.Allocator, writes: []const db_types.BatchWrite) ![]db_types.BatchWrite {
            return allocator.dupe(db_types.BatchWrite, writes);
        }
        fn openNormalize(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?View {
            return .{ .ptr = ptr, .vtable = &.{ .next = undefined, .normalize = normalize, .close = close } };
        }
        fn close(_: *anyopaque) void {}
        fn commit(ptr: *anyopaque, allocator: std.mem.Allocator, _: db_types.TxnId, _: u64, tables: []const contract.TableCommitRequest, _: db_types.SyncLevel) !?contract.CommitOutcome {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), tables.len);
            const table = tables[0];
            try std.testing.expectEqualStrings("physical_usage", table.table_name);
            try std.testing.expectEqual(@as(usize, 1), table.writes.len);
            try std.testing.expectEqualStrings("stored-1", table.writes[0].key);
            try std.testing.expect(table.range_guards.len != 0);
            if (self.absence_case) {
                var saw_absence = false;
                for (table.range_guards) |guard| for (guard.proofs) |proof| if (proof.bucket == 99) {
                    saw_absence = true;
                };
                try std.testing.expect(saw_absence);
            }
            try std.testing.expect(table.integrity_commands.len != 0);
            try std.testing.expectEqualDeep(self.generation_set, table.relational_integrity_generation_set.?);
            var saw_owner = false;
            for (table.integrity_commands) |command| switch (command.operation) {
                .compare_claim => |maybe_owner| if (maybe_owner) |owner| {
                    try std.testing.expectEqualStrings("stored-1", owner.parent_key);
                    try std.testing.expectEqualStrings("physical_usage", owner.parent_table);
                    saw_owner = true;
                },
                else => {},
            };
            try std.testing.expect(saw_owner);
            const row = try std.json.parseFromSlice(std.json.Value, allocator, table.writes[0].value, .{});
            defer row.deinit();
            if (self.expect_status) {
                try std.testing.expectEqualStrings("ready", row.value.object.get("status").?.string);
            } else if (self.absence_case) {
                try std.testing.expect(row.value.object.get("status").? == .null);
            } else try std.testing.expectEqual(@as(i64, 8), row.value.object.get("quantity").?.integer);
            if (self.phantom_on_commit) return .{ .conflict = .{ .table_name = "physical_usage", .key = "stored-1", .message = "range generation advanced", .retryable = true } };
            self.commits += 1;
            return .{ .committed = .{ .participant_count = 1 } };
        }
    };
    const envelope = try integrity.testCatalogEnvelope(alloc, 3, Fake.schema);
    defer alloc.free(envelope);
    const parsed = try std.json.parseFromSlice(struct { catalog: []const u8 }, alloc, envelope, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const decoded = try alloc.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(parsed.value.catalog));
    defer alloc.free(decoded);
    try std.base64.standard.Decoder.decode(decoded, parsed.value.catalog);
    var native = try native_catalog.decode(alloc, decoded);
    defer native.deinit();
    var fake: Fake = .{ .envelope = envelope, .digest = native.schema_digest, .generation_set = activation.generationSet(native) };
    const source: reads.TableReadSource = .{ .ptr = &fake, .supports_sql_range_guards = true, .vtable = &.{ .lookup = Fake.lookup, .scan = undefined, .query = undefined, .open_relational_statement = Fake.open, .open_relational_statement_snapshot = Fake.openSnapshot, .open_relational_read = Fake.openNormalize } };
    var tuple_arena = std.heap.ArenaAllocator.init(alloc);
    defer tuple_arena.deinit();
    fake.tuple = try alloc.dupe(u8, try integrity.testConflictTuple(tuple_arena.allocator(), source, &fake.records, "physical_usage", &.{"id"}, .{ .key = "proposed-1", .value = "{\"id\":\"u1\",\"status\":\"ready\",\"quantity\":1}" }));
    defer alloc.free(fake.tuple);
    fake.claim = true;
    var backend_runtime = try @import("antfly_local_sources").storage_background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
    defer backend_runtime.deinit();
    var prepared_backend = @import("antfly_local_sources").storage_mem_backend.Backend.init(alloc, .{});
    defer prepared_backend.close();
    var prepared_store = try prepared_backend.runtimeStore(alloc, .{ .name = "native-conflict-capture-test" });
    defer prepared_store.deinit();
    var durable = @import("transactions.zig").DurableSessionStore.initRuntime(alloc, &prepared_store);
    var server = http_server.ApiHttpServer.init(alloc, .{ .backend_runtime = backend_runtime.ptr(), .session_store = &durable }, .{ .ptr = &fake, .vtable = &.{ .status = Fake.status, .system_catalog = Fake.resolve, .supports_query_definitions = true, .admin_snapshot = Fake.snapshot, .free_admin_snapshot = Fake.freeSnapshot } }, source, .{ .ptr = &fake, .supports_sql_range_guards = true, .vtable = &.{ .batch = undefined, .commit_transaction_with_id = Fake.commit } });
    defer server.deinit();
    var permissions = [_]@import("../usermgr/mod.zig").Permission{ .{ .resource = @constCast("*"), .resource_type = .table, .type = .read }, .{ .resource = @constCast("*"), .resource_type = .table, .type = .write } };
    var identity = try http_server.cloneCatalogIdentity(alloc, .{ .username = @constCast("native_conflict_writer"), .permissions = &permissions });
    defer if (identity) |*owned| owned.deinit(alloc);
    var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{} };
    const corpus = try std.json.parseFromSlice(std.json.Value, alloc, @import("antfly_local_sources").sql_parity_fixtures.inventory, .{});
    defer corpus.deinit();
    const sql = for (corpus.value.object.get("entries").?.array.items) |entry| {
        if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-1411")) break entry.object.get("sql").?.string;
    } else return error.TestMissingCorpusCase;
    var compiled = try compiler.compile(alloc, sql, .{});
    defer compiled.deinit();
    var result = try adapter.execute(alloc, &compiled, &.{}, .{}, null);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), fake.captures);
    try std.testing.expectEqual(@as(usize, 1), fake.point_reads);
    try std.testing.expectEqual(@as(usize, 1), fake.publication_reads);
    try std.testing.expectEqual(@as(usize, 2), fake.dynamic_opens);
    try std.testing.expectEqual(@as(usize, 1), fake.commits);
    try std.testing.expectEqualStrings("u1", result.output.rows[0][0].string);
    const select_sql = for (corpus.value.object.get("entries").?.array.items) |entry| {
        if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-1440")) break entry.object.get("sql").?.string;
    } else return error.TestMissingCorpusCase;
    fake.expect_status = true;
    var select_compiled = try compiler.compile(alloc, select_sql, .{});
    defer select_compiled.deinit();
    var select_result = try adapter.execute(alloc, &select_compiled, &.{}, .{}, null);
    defer select_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), fake.captures);
    try std.testing.expectEqual(@as(usize, 2), fake.point_reads);
    try std.testing.expectEqual(@as(usize, 2), fake.publication_reads);
    try std.testing.expectEqual(@as(usize, 5), fake.dynamic_opens);
    try std.testing.expectEqual(@as(usize, 2), fake.commits);
    try std.testing.expectEqualStrings("u1", select_result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("ready", select_result.output.rows[0][1].string);
    fake.phantom_on_commit = true;
    try std.testing.expectError(error.SqlWriteConflict, adapter.execute(alloc, &select_compiled, &.{}, .{}, null));
    try std.testing.expectEqual(@as(usize, 2), fake.commits);
    fake.expect_status = false;
    fake.absence_case = true;
    var absent_compiled = try compiler.compile(alloc, "INSERT INTO usage_records (id, status, quantity) VALUES ('u1', 'ready', 1) ON CONFLICT (id) DO UPDATE SET status = (SELECT status FROM usage_records WHERE id = 'missing')", .{});
    defer absent_compiled.deinit();
    try std.testing.expectError(error.SqlWriteConflict, adapter.execute(alloc, &absent_compiled, &.{}, .{}, null));
    try std.testing.expectEqual(@as(usize, 2), fake.commits);
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

test "SQL stateless completion reserves before native entry and owns ambiguous response metadata" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    const background = @import("antfly_local_sources").storage_background_runtime;
    const scheduler = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const runtime = @import("antfly_runtime_abi");
    const distributed = @import("distributed_txn.zig");
    const platform_time = @import("antfly_platform").time;
    const public = @import("public_table_http.zig");
    const Fake = struct {
        expected: distributed.CommitControl = .{},
        scheduling: *scheduler.Scheduler,
        signal: *std.atomic.Value(bool),
        scenario: usize,
        calls: usize = 0,
        records: [1]@import("antfly_local_sources").common_topology_records.TableRecord = .{.{ .table_id = 3, .name = "docs", .schema_json = "{\"version\":7}" }},
        fn snapshot(raw: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .status = try status(raw), .tables = &self.records, .ranges = &.{}, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn freeSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
        var completion_now: std.atomic.Value(u64) = .init(0);
        fn now(_: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            return .{ .nanoseconds = completion_now.load(.acquire) };
        }
        fn canceled(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_types.BatchRequest) !?void {
            return error.UnexpectedSynchronousBatch;
        }
        fn commit(raw: *anyopaque, _: std.mem.Allocator, tables: []const distributed.TableCommitRequest, _: db_types.SyncLevel, cancellation: db_types.CancellationToken, control: distributed.CommitControl) !?distributed.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqualDeep(self.expected, control);
            try std.testing.expectEqualDeep(self.expected.pre_decision.cancellation, cancellation);
            try std.testing.expectEqualStrings("docs", tables[0].table_name);
            try std.testing.expectEqualStrings("a", tables[0].writes[0].key);
            try std.testing.expectEqualStrings("{\"id\":1}", tables[0].writes[0].value);
            try std.testing.expectEqual(@as(?u32, 7), tables[0].schema_version);
            const borrow = control.team.?.borrow;
            const initial = borrow.checkpoint(borrow.context);
            if (!initial.isOk()) return runtime.error_abi.errorFromStatus(initial);
            self.calls += 1;
            if (self.scenario == 2) {
                self.signal.store(true, .release);
                const parked = borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource));
                if (!parked.isOk()) return runtime.error_abi.errorFromStatus(parked);
                try std.testing.expectEqual(@as(usize, 0), self.scheduling.stats().runnable_slots);
                var health: scheduler.Execution = .{};
                try health.start(self.scheduling, .{ .io = std.testing.io, .allow_wait = false }, .control);
                health.finish();
                self.scheduling.close();
                const resumed = borrow.enter(borrow.context, @backingInt(runtime.runnable_abi.Class.write));
                if (!resumed.isOk()) return runtime.error_abi.errorFromStatus(resumed);
                const completed = borrow.checkpoint(borrow.context);
                if (!completed.isOk()) return runtime.error_abi.errorFromStatus(completed);
            }
            if (self.scenario == 5) {
                public.setLastAmbiguousBatchTxnId("00112233445566778899aabbccddeeff".*);
                return error.CommitDecisionUnknown;
            }
            if (self.scenario == 6) {
                completion_now.store(31 * std.time.ns_per_s, .release);
                const expired = borrow.checkpoint(borrow.context);
                if (!expired.isOk()) {
                    public.setLastAmbiguousBatchTxnId("00112233445566778899aabbccddeeff".*);
                    return error.CommitDecisionUnknown;
                }
                return error.ExpectedCompletionExpiry;
            }
            return .{ .committed = .{ .participant_count = 1 } };
        }
    };
    const Forward = struct {
        const Dispatch = @FieldType(runtime.native_abi.IoBorrow, "dispatch");
        var owner_dispatch: Dispatch = undefined;
        var calls: std.atomic.Value(usize) = .init(0);
        const P = @typeInfo(@typeInfo(Dispatch).pointer.child).@"fn".param_types;
        fn dispatch(a: P[0].?, b: P[1].?, c: P[2].?, d: P[3].?, e: P[4].?, f: P[5].?) callconv(.c) void {
            _ = calls.fetchAdd(1, .monotonic);
            owner_dispatch(a, b, c, d, e, f);
        }
    };
    // Exercise the same admission, cancellation, expiry and ambiguous-outcome
    // matrix with native IO, foreign C-dispatch IO, and unavailable durable IO.
    for (0..3) |mode| {
        for (0..8) |scenario| {
            const alloc = std.testing.allocator;
            var threaded = std.Io.Threaded.init(alloc, .{ .concurrent_limit = .limited(if (scenario == 4) 0 else 2) });
            defer threaded.deinit();
            Fake.completion_now.store(0, .release);
            var io_vtable = threaded.io().vtable.*;
            io_vtable.now = Fake.now;
            const io: std.Io = .{ .userdata = threaded.io().userdata, .vtable = &io_vtable };
            var imported_borrow = runtime.native_abi.IoBorrow.init(&io);
            Forward.owner_dispatch = imported_borrow.dispatch;
            Forward.calls.store(0, .release);
            imported_borrow.dispatch = Forward.dispatch;
            var receiver = try imported_borrow.receive();
            const imported_io = receiver.io();
            try std.testing.expect(imported_io.vtable != io.vtable);
            var backend = try background.BackendRuntime.init(alloc, .{ .backend = .manual, .borrowed_io = .{ .general = io } });
            defer backend.deinit();
            var scheduling = try scheduler.Scheduler.init(.{ .slots = 3 });
            var execution: scheduler.Execution = .{};
            defer execution.finish();
            var signal = std.atomic.Value(bool).init(false);
            const original_deadline = platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
            try execution.start(&scheduling, .{ .io = std.testing.io, .deadline_ns = original_deadline, .native_now_ns = platform_time.monotonicNs, .cancellation = .{ .ptr = &signal, .requested = Fake.canceled } }, .write);
            var memory: @import("../common/allocation_budget.zig").Shared = .{ .child = alloc, .limit = if (scenario == 0) 0 else 16384 };
            var bridge = try @import("runnable_write_team.zig").Completion.init(&execution, memory.allocator());
            var team = try bridge.team();
            var fake = Fake{ .scheduling = &scheduling, .signal = &signal, .scenario = scenario };
            var server = http_server.ApiHttpServer.init(alloc, .{ .backend_runtime = &backend }, .{ .ptr = &fake, .vtable = &.{ .status = Fake.status, .admin_snapshot = Fake.snapshot, .free_admin_snapshot = Fake.freeSnapshot } }, null, .{ .ptr = &fake, .vtable = &.{ .batch = Fake.batch, .commit_batch_with_control = Fake.commit } });
            defer server.deinit();
            var identity: ?http_server.AuthenticatedIdentity = null;
            var adapter = Adapter{ .server = &server, .identity = &identity, .context = .{ .execution_team = &team, .deadline_ns = original_deadline, .cancellation = .fromAtomic(&signal) } };
            if (scenario == 7) adapter.context.deadline_ns = platform_time.monotonicNs() -| 1;
            fake.expected = distributed.CommitControl.fromRequest(adapter.context);
            if (scenario == 3) signal.store(true, .release);
            if (mode != 0) {
                server.cfg.imported_runtime_io = .{ .api = io, .api_network = io, .api_filesystem = io, .durable = if (mode == 1) imported_io else null };
                // All setup and unrelated teardown retain their ordinary owner.
                // Only completion executes with a deliberately inaccessible pointer.
                server.cfg.backend_runtime = @ptrFromInt(0x1000);
            }
            const result = adapter.completeNativeBatch(alloc, "docs", .{ .schema_version = 7, .writes = &.{.{ .key = "a", .value = "{\"id\":1}" }} });
            server.cfg.backend_runtime = &backend;
            if (mode == 2 and scenario != 3 and scenario != 7) {
                try std.testing.expectError(error.SqlWriteCapacityUnavailable, result);
            } else if (scenario == 0) {
                try std.testing.expectError(error.OutOfMemory, result);
            } else if (scenario == 3) {
                try std.testing.expectError(error.Canceled, result);
            } else if (scenario == 4) {
                try std.testing.expectError(error.ConcurrencyUnavailable, result);
            } else if (scenario == 7) {
                try std.testing.expectError(error.DeadlineExceeded, result);
            } else {
                var response = try result;
                defer response.deinit(alloc);
                if (scenario == 5 or scenario == 6) {
                    try std.testing.expectEqual(@as(u16, 409), response.status);
                    const failure = classifyMutationFailure(response.status, response.body);
                    try std.testing.expectEqual(error.SqlMutationOutcomeUnknown, failure.err);
                    try std.testing.expectEqualDeep(@as(?[32]u8, "00112233445566778899aabbccddeeff".*), failure.transaction_id);
                } else try std.testing.expectEqual(@as(u16, 201), response.status);
            }
            try std.testing.expectEqual(@as(usize, if (mode == 2 or scenario == 0 or scenario == 3 or scenario == 4 or scenario == 7) 0 else 1), fake.calls);
            if (mode == 1 and scenario != 3 and scenario != 7) try std.testing.expect(Forward.calls.load(.acquire) != 0);
            if (mode == 2) try std.testing.expectEqual(@as(usize, 0), Forward.calls.load(.acquire));
            try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
            try std.testing.expectEqual(@as(usize, 1), scheduling.stats().operations);
        }
    }
}

test "SQL read resume fences cleanup resource waits retain independent deadline and original class" {
    const runtime = @import("antfly_runtime_abi");
    const Clock = struct {
        var current: u64 = 0;
        fn now() u64 {
            return current;
        }
    };
    for ([_]http_server.RunnableScheduler.Class{ .query, .write }) |class| {
        Clock.current = 0;
        var scheduler = try http_server.RunnableScheduler.Scheduler.init(.{ .slots = 3, .max_operations = 4, .max_bytes = 128, .control_bytes = 32 });
        defer scheduler.close();
        var execution: http_server.RunnableScheduler.Execution = .{};
        defer execution.finish();
        try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 10, .retained_bytes = 17 }, class);
        var canceled: std.atomic.Value(bool) = .init(false);
        var adapter: Adapter = .{ .server = undefined, .identity = undefined, .context = .{ .cancellation = operation.CancellationToken.fromAtomic(&canceled) }, .runnable = &execution };
        const borrow = adapter.providerRunnable().?;
        const io = std.testing.io;
        const io_borrow = runtime.native_abi.IoBorrow.init(&io);
        try Adapter.checkCleanupStatus(borrow.reserve_cleanup.?(borrow.context, &.{ .io = &io_borrow, .native_deadline = 1, .activation_budget = 1, .deadline_ns = 10 }));
        // Both original request cancellation and original scheduler deadline
        // expire before cleanup activation. Cleanup has its own finite budget.
        canceled.store(true, .release);
        adapter.context.deadline_ns = 0;
        Clock.current = 1000;
        try std.testing.expectError(error.Canceled, adapter.context.ensureActive());
        var cleanup_slots: usize = 0;
        try Adapter.checkCleanupStatus(borrow.activate_cleanup.?(borrow.context, &cleanup_slots));
        try std.testing.expectEqual(execution.entry.?.slots, cleanup_slots);
        try std.testing.expectEqual(class, execution.entry.?.class);
        try std.testing.expectEqual(@as(?u64, 1010), execution.entry.?.options.deadline_ns);
        try Adapter.checkpoint(&adapter);
        try Adapter.spillWaitBegin(&adapter);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 17), scheduler.stats().retained_bytes);
        var control: http_server.RunnableScheduler.Execution = .{};
        try control.start(&scheduler, .{ .io = std.testing.io }, .control);
        control.finish();
        Clock.current = 1009;
        try Adapter.spillWaitEnd(&adapter);
        try std.testing.expectEqual(class, execution.entry.?.class);
        try Adapter.checkpoint(&adapter);
        try std.testing.expectEqual(@as(?u64, 1010), execution.entry.?.options.deadline_ns);
        Clock.current = 1010;
        try std.testing.expectError(error.DeadlineExceeded, Adapter.checkpoint(&adapter));
        try std.testing.expectError(error.DeadlineExceeded, Adapter.spillWaitBegin(&adapter));
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 17), scheduler.stats().retained_bytes);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "SQL lake statement wrapper preserves ordering counts estimates and split source lifetime" {
    const a = std.testing.allocator;
    const Fixture = struct {
        statement_closed: bool = false,
        children: usize = 0,
        fn next(_: *anyopaque, _: std.mem.Allocator, _: u32) !catalog.Page {
            return .{ .rows = &.{} };
        }
        fn count(raw: *anyopaque) !?u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!self.statement_closed);
            return 12345;
        }
        fn childClose(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(!self.statement_closed);
            self.children -= 1;
        }
        fn split(raw: *anyopaque, alloc: std.mem.Allocator, maximum: usize) !?[]catalog.Cursor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!self.statement_closed);
            const children = try alloc.alloc(catalog.Cursor, @min(maximum, 2));
            for (children) |*child| child.* = .{ .ptr = self, .next = next, .close = childClose, .count_rows = count };
            self.children += children.len;
            return children;
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.children == 0);
            self.statement_closed = true;
        }
    };
    var fixture: Fixture = .{};
    const physical: catalog.Cursor = .{ .ptr = &fixture, .next = Fixture.next, .close = Fixture.close, .order_satisfied = true, .estimated_rows = 12345, .estimated_bytes = 98765, .ordered_split_bytes = 64, .count_rows = Fixture.count, .split_scan = Fixture.split, .split_ordered = Fixture.split };
    const owner = try a.create(Adapter.SingleStatementCursor);
    owner.* = .{ .alloc = a, .statement = .{ .ptr = &fixture, .cursors = &.{physical}, .close = Fixture.close } };
    const wrapped = owner.asCursor();
    try std.testing.expect(wrapped.order_satisfied);
    try std.testing.expectEqual(physical.estimated_rows, wrapped.estimated_rows);
    try std.testing.expectEqual(physical.estimated_bytes, wrapped.estimated_bytes);
    try std.testing.expectEqual(physical.ordered_split_bytes, wrapped.ordered_split_bytes);
    try std.testing.expect(wrapped.next_columns == null and wrapped.set_dynamic_filter == null);
    try std.testing.expectEqual(@as(?u64, 12345), try wrapped.count_rows.?(wrapped.ptr));
    for ([_]*const fn (*anyopaque, std.mem.Allocator, usize) anyerror!?[]catalog.Cursor{ wrapped.split_scan.?, wrapped.split_ordered.? }) |split| {
        const children = (try split(wrapped.ptr, a, 4)).?;
        for (children) |child| {
            try std.testing.expectEqual(@as(?u64, 12345), try child.count_rows.?(child.ptr));
            child.close(child.ptr);
        }
        a.free(children);
    }
    wrapped.close(wrapped.ptr);
    try std.testing.expect(fixture.statement_closed);
}

test "lake SQL failed aggregate admission releases control before its descriptor" {
    const FailedOpen = struct {
        fn open(a: std.mem.Allocator) !void {
            const owner = try a.create(Adapter.AggregateArtifactCursor);
            owner.a = a;
            owner.reader_lease = null;
            owner.arena = .init(a);
            owner.final_page = .init(a);
            defer owner.destroyControl();
            _ = try owner.arena.allocator().alloc(u8, 128);
            return error.NativeArtifactStorageRequired;
        }
    };
    try std.testing.expectError(error.NativeArtifactStorageRequired, FailedOpen.open(std.testing.allocator));
}

test "SQL receipt polling expiration preserves caller cancellation and statement deadlines" {
    try std.testing.expectEqual(error.ReadRequirementNotReady, Adapter.receiptPollingError(.{}, 0, error.DeadlineExceeded));
    try std.testing.expectEqual(error.DeadlineExceeded, Adapter.receiptPollingError(.{ .deadline_ns = 0 }, 0, error.DeadlineExceeded));
    var signal = std.atomic.Value(bool).init(true);
    try std.testing.expectEqual(error.Canceled, Adapter.receiptPollingError(.{ .cancellation = operation.CancellationToken.fromAtomic(&signal) }, 0, error.DeadlineExceeded));
    try std.testing.expectEqual(error.LakeCatalogForbidden, Adapter.receiptPollingError(.{}, 0, error.LakeCatalogForbidden));
}

test "SQL receipt readiness shares one deadline across preflight and scan checks" {
    var budget: ReceiptWaitBudget = .{};
    const start: u64 = 100 * std.time.ns_per_ms;
    const deadline = budget.deadline(start, 10);
    try std.testing.expectEqual(start + 10 * std.time.ns_per_ms, deadline);
    // Time spent obtaining the preflight result remains spent when the scan
    // checks readiness again, including after the original deadline expires.
    try std.testing.expectEqual(deadline, budget.deadline(start + 9 * std.time.ns_per_ms, 10));
    try std.testing.expectEqual(deadline, budget.deadline(start + 11 * std.time.ns_per_ms, 10));
    budget = .{};
    try std.testing.expectEqual(start + 30 * std.time.ns_per_ms, budget.deadline(start + 20 * std.time.ns_per_ms, 10));
    budget = .{};
    try std.testing.expectEqual(start, budget.deadline(start, 0));
    try std.testing.expect(budget.deadline_ns == null);
    try std.testing.expectEqual(std.math.maxInt(u64), budget.deadline(std.math.maxInt(u64) - 1, 10));
}

test "SQL read resume fences receipt polling sleep lends CPU and retains the original owner" {
    const Runnable = http_server.RunnableScheduler;
    const Case = enum { success, io_canceled, request_canceled, receipt_expired, shutdown, canceled_before, expired_before };
    const Fixture = struct {
        scheduler: *Runnable.Scheduler,
        wait: *Adapter.ReceiptPollWait,
        canceled: *std.atomic.Value(bool),
        scenario: Case,
        calls: usize = 0,
        parked: bool = false,
        other_granted: bool = false,
        control_granted: bool = false,
        retained_operations: usize = 0,
        retained_bytes: u64 = 0,

        fn cancellation(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }

        fn sleep(raw: ?*anyopaque, _: std.Io.Timeout) std.Io.Cancelable!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            const before = self.scheduler.stats();
            self.parked = before.runnable_slots == 0 and before.resource_blocked == 1;
            self.retained_operations = before.operations;
            self.retained_bytes = before.retained_bytes;
            // Submit without waiting: a regression must fail rather than hang
            // behind the polling operation's unreleased ordinary CPU slot.
            var other: Runnable.Entry = .{ .options = .{ .io = std.testing.io }, .class = .query };
            self.scheduler.submit(&other) catch return error.Canceled;
            self.other_granted = other.state == .running;
            self.scheduler.finish(&other);
            var control: Runnable.Entry = .{ .options = .{ .io = std.testing.io }, .class = .control };
            self.scheduler.submit(&control) catch return error.Canceled;
            self.control_granted = control.state == .running;
            self.scheduler.finish(&control);
            switch (self.scenario) {
                .io_canceled => return error.Canceled,
                .request_canceled => self.canceled.store(true, .release),
                .receipt_expired => self.wait.deadline_ns = 0,
                .shutdown => self.scheduler.close(),
                else => {},
            }
        }
    };
    for (std.enums.values(Case)) |scenario| {
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 2, .control_slots = 1, .max_operations = 4, .max_bytes = 128, .control_bytes = 32 });
        defer scheduler.close();
        var canceled = std.atomic.Value(bool).init(false);
        const deadline = @import("antfly_platform").time.monotonicNs() +| 30 * std.time.ns_per_s;
        var execution: Runnable.Execution = .{};
        defer execution.finish();
        try execution.start(&scheduler, .{ .io = std.testing.io, .deadline_ns = deadline, .native_now_ns = @import("antfly_platform").time.monotonicNs, .retained_bytes = 17, .cancellation = .{ .ptr = &canceled, .requested = Fixture.cancellation } }, .query);
        const initial_generation = execution.resume_generation;
        var identity: ?http_server.AuthenticatedIdentity = null;
        // A sleep with no captured read pins does not access the server.
        var adapter: Adapter = .{ .server = undefined, .identity = &identity, .context = .{ .deadline_ns = deadline, .cancellation = .fromAtomic(&canceled) }, .runnable = &execution };
        var wait: Adapter.ReceiptPollWait = .{ .adapter = &adapter, .deadline_ns = deadline };
        var fixture: Fixture = .{ .scheduler = &scheduler, .wait = &wait, .canceled = &canceled, .scenario = scenario };
        var vtable = std.testing.io.vtable.*;
        vtable.sleep = Fixture.sleep;
        const io: std.Io = .{ .userdata = &fixture, .vtable = &vtable };
        if (scenario == .canceled_before) canceled.store(true, .release);
        if (scenario == .expired_before) wait.deadline_ns = 0;
        const result = wait.sleep(io, .fromMilliseconds(1));
        switch (scenario) {
            .success => try result,
            .io_canceled, .request_canceled, .canceled_before => try std.testing.expectError(error.Canceled, result),
            .receipt_expired, .expired_before => try std.testing.expectError(error.ReadRequirementNotReady, result),
            .shutdown => try std.testing.expectError(error.SchedulerClosed, result),
        }
        const called = scenario != .canceled_before and scenario != .expired_before;
        try std.testing.expectEqual(@as(usize, if (called) 1 else 0), fixture.calls);
        if (called) {
            try std.testing.expect(fixture.parked);
            try std.testing.expect(fixture.other_granted);
            try std.testing.expect(fixture.control_granted);
            try std.testing.expectEqual(@as(usize, 1), fixture.retained_operations);
            try std.testing.expectEqual(@as(u64, 17), fixture.retained_bytes);
        }
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 17), scheduler.stats().retained_bytes);
        try std.testing.expectEqual(@as(?u64, deadline), execution.entry.?.options.deadline_ns);
        try std.testing.expectEqual(Runnable.Class.query, execution.entry.?.class);
        if (scenario == .success or scenario == .io_canceled) {
            try std.testing.expectEqual(Runnable.State.running, execution.entry.?.state);
            try std.testing.expectEqual(initial_generation + 1, execution.resume_generation);
        }
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        std.debug.print("receipt polling sleep case {s}: original owner retired without runnable or metadata charges\n", .{@tagName(scenario)});
    }
}

test "SQL retained cursor admission lends original CPU and prevents replay after rejected pages" {
    const a = std.testing.allocator;
    const scheduler = http_server.RunnableScheduler;
    const clock = @import("antfly_platform").time;
    const View = @import("antfly_local_sources").api_table_read_source.RelationalReadView;
    const Mode = enum { success, canceled, expired, rejected_resume, late_cancel };
    const Fixture = struct {
        adapter: *Adapter,
        scheduler: *scheduler.Scheduler,
        signal: *std.atomic.Value(bool),
        mode: Mode,
        calls: usize = 0,
        fn canceled(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
        fn legacy(_: *anyopaque, _: std.mem.Allocator, _: u32) !View.Page {
            return error.TestLegacyCursorEscape;
        }
        fn close(_: *anyopaque) void {}
        fn next(raw: *anyopaque, alloc: std.mem.Allocator, _: u32, control: View.Admission) !View.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try control.checkpoint();
            try std.testing.expectEqual(self.adapter.context.deadline_ns, try control.deadlineNs());
            try control.waitForProgress();
            const wait = (try control.executionWait()) orelse return error.TestMissingSqlExecution;
            try wait.beginWait();
            try control.checkLifetime();
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 17), self.scheduler.stats().retained_bytes);
            var health: scheduler.Execution = .{};
            try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            health.finish();
            switch (self.mode) {
                .canceled => self.signal.store(true, .release),
                .expired => self.adapter.context.deadline_ns = 0,
                .rejected_resume => self.scheduler.close(),
                .success, .late_cancel => {},
            }
            try wait.endWait();
            if (self.mode == .late_cancel) self.signal.store(true, .release);
            return .{ .arena = .init(alloc), .rows = &.{}, .after = null };
        }
    };
    for ([_]Mode{ .success, .canceled, .expired, .rejected_resume, .late_cancel }) |mode| {
        var scheduling = try scheduler.Scheduler.init(.{ .slots = 2 });
        var signal = std.atomic.Value(bool).init(false);
        var execution: scheduler.Execution = .{};
        const deadline = clock.monotonicNs() + std.time.ns_per_s;
        try execution.start(&scheduling, .{ .io = std.testing.io, .allow_wait = false, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 17, .cancellation = .{ .ptr = &signal, .requested = Fixture.canceled } }, .query);
        defer execution.finish();
        // An empty guard deliberately needs no catalog callback, but the real
        // guard/resume machinery and original execution remain active.
        var server: http_server.ApiHttpServer = undefined;
        server.alloc = a;
        var adapter: Adapter = .{ .server = &server, .identity = undefined, .runnable = &execution, .context = .{ .deadline_ns = deadline, .cancellation = .fromAtomic(&signal), .fanout_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&std.testing.io) } };
        try adapter.beginReadFence();
        defer adapter.endReadFence();
        var fixture: Fixture = .{ .adapter = &adapter, .scheduler = &scheduling, .signal = &signal, .mode = mode };
        var projection = try @import("antfly_local_sources").sql_document_row.Projection.init(a, .{ .id = 1, .physical_name = "rows", .schema_version = 5, .storage_mode = .document, .columns = &.{} }, &.{"_id"});
        defer projection.deinit(a);
        var cursor: Adapter.ReadCursor = .{ .alloc = a, .adapter = &adapter, .schema_version = 5, .projection = projection, .view = .{ .ptr = &fixture, .vtable = &.{ .next = Fixture.legacy, .next_with_admission = Fixture.next, .close = Fixture.close } } };
        const result = Adapter.ReadCursor.next(&cursor, a, 1);
        if (mode == .success) {
            (try result).deinit();
            execution.finish();
            var second: scheduler.Execution = .{};
            try second.start(&scheduling, .{ .io = std.testing.io, .allow_wait = false, .native_now_ns = clock.monotonicNs, .deadline_ns = deadline, .retained_bytes = 17 }, .query);
            defer second.finish();
            adapter.runnable = &second;
            (try Adapter.ReadCursor.next(&cursor, a, 1)).deinit();
            try std.testing.expectEqual(@as(usize, 2), fixture.calls);
        } else {
            try std.testing.expectError(switch (mode) {
                .canceled, .late_cancel => error.Canceled,
                .expired => error.DeadlineExceeded,
                .rejected_resume => error.SchedulerClosed,
                .success => unreachable,
            }, result);
            try std.testing.expect(cursor.failed);
            try std.testing.expectError(error.RetainedReadRestartRequired, Adapter.ReadCursor.next(&cursor, a, 1));
            try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        }
    }
}

test "SQL lake request admission lends CPU for artifact catalog WAL and pin providers" {
    const a = std.testing.allocator;
    const local = @import("antfly_local_sources");
    const scheduling = http_server.RunnableScheduler;
    const clock = @import("antfly_platform").time;
    const Context = local.serverless_query_lake_read_context.Context;
    const Mode = enum { success, canceled, expired, rejected_resume, forbidden };
    const Stage = enum { artifact, catalog, wal, pin };
    const Probe = struct {
        adapter: *Adapter,
        scheduler: *scheduling.Scheduler,
        signal: *std.atomic.Value(bool),
        context: Context = .{},
        mode: Mode,
        enabled: bool = false,
        denied: bool = false,
        ends: usize = 0,
        reject_at: usize,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try Adapter.checkpoint(self.adapter);
            if (self.denied) return error.Forbidden;
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try Adapter.spillWaitBegin(self.adapter);
            // The SQL-owned authority hook must remain valid without CPU.
            try self.context.checkLifetime();
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 17), self.scheduler.stats().retained_bytes);
            var health: scheduling.Execution = .{};
            try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            health.finish();
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.enabled) {
                self.ends += 1;
                if (self.ends == self.reject_at) switch (self.mode) {
                    .success => {},
                    .canceled => self.signal.store(true, .release),
                    .expired => self.adapter.context.deadline_ns = 0,
                    .rejected_resume => self.scheduler.close(),
                    .forbidden => self.denied = true,
                };
            }
            try Adapter.spillWaitEnd(self.adapter);
        }
    };
    const Operation = struct {
        fn run(arena: std.mem.Allocator, stage: Stage, store: *@import("lake_index_store.zig").Store, context: Context, token: []const u8, identity: [32]u8) !void {
            switch (stage) {
                .artifact => {
                    var artifacts = store.artifactStore();
                    const cut = try @import("lake_sql_retained_cut.zig").loadFor(arena, &artifacts, identity, token, .{ .id = 7, .physical_name = "docs", .schema_version = 1, .columns = &.{} }, 1001, .none, context);
                    try std.testing.expectEqualStrings("42", cut.snapshot_id);
                    try std.testing.expectEqual(@as(u64, 8), cut.pending.lsn);
                },
                .catalog => {
                    var managed: local.serverless_external_source_mod.lake_catalog.managed.Managed = .{ .client = store.opened.client, .bucket = store.opened.bucket, .prefix = "catalog", .source_uri = "file:///fixture", .context = context };
                    var table = try managed.load(arena);
                    defer table.deinit(arena);
                    try std.testing.expect(table.metadata_json.len != 0);
                },
                .wal => {
                    const wal: @import("../serverless/lake_wal.zig").Store = .{ .client = store.opened.client, .bucket = store.opened.bucket, .prefix = "wal", .context = context };
                    try std.testing.expectEqual(null, try wal.latest(arena));
                },
                .pin => {
                    const pins: @import("../serverless/lake_snapshot_pins.zig").Store = .{ .client = store.opened.client, .bucket = store.opened.bucket, .prefix = "pins", .context = context };
                    const now = clock.realtimeNs();
                    const deadline = try pins.acquireFor(arena, "42", now, std.time.ns_per_s);
                    try std.testing.expect(deadline >= now);
                },
            }
        }
    };
    for ([_]Stage{ .artifact, .catalog, .wal, .pin }) |stage| for ([_]Mode{ .success, .canceled, .expired, .rejected_resume, .forbidden }) |mode| {
        if (mode == .forbidden and stage != .wal) continue;
        var directory = try local.common_test_directory.TestDirectory.init("sql-lake-call-admission");
        defer directory.cleanup();
        var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
        var execution: scheduling.Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 17 }, .query);
        defer execution.finish();
        var signal = std.atomic.Value(bool).init(false);
        var server: http_server.ApiHttpServer = undefined;
        server.alloc = a;
        server.embedding_provider_runtime.io = std.testing.io;
        var adapter: Adapter = .{ .server = &server, .identity = undefined, .runnable = &execution, .context = .{ .deadline_ns = clock.monotonicNs() + 10 * std.time.ns_per_s, .cancellation = .fromAtomic(&signal) } };
        try adapter.beginReadFence();
        defer adapter.endReadFence();
        var probe: Probe = .{ .adapter = &adapter, .scheduler = &scheduler, .signal = &signal, .mode = mode, .reject_at = if (stage == .pin) 3 else 1 };
        probe.context = try adapter.lakeReadContext(null);
        try std.testing.expect(probe.context.wait != null);
        probe.context.wait = .{ .ptr = &probe, .begin = Probe.begin, .end = Probe.end };
        probe.context.additional_checkpoint = .{ .ptr = &probe, .check = Probe.check };
        var store = try @import("lake_index_store.zig").Store.openNativeFor(a, null, null, false, .standalone, directory.path(), probe.context);
        defer store.deinit();
        const identity = try @import("native_retained_cut.zig").storeIdentity(a, store.locator);
        var artifacts = store.artifactStore();
        const token = try @import("lake_sql_retained_cut.zig").save(a, &artifacts, identity, std.testing.io, .{ .table_id = 7, .schema_version = 1, .object_generation = 0, .expires_ms = 1000 + @import("lake_sql_retained_cut.zig").ttl_ms, .metadata_location = "file:///fixture/metadata.json", .metadata_json = "{}", .table_uuid = "fixture", .snapshot_id = "42", .pending = .{ .lsn = 8, .key_fields = &.{"id"}, .changes = &.{} } }, .none);
        defer a.free(token);
        if (stage == .catalog) {
            // Seed a real filesystem catalog before testing its read boundary.
            var managed: local.serverless_external_source_mod.lake_catalog.managed.Managed = .{ .client = store.opened.client, .bucket = store.opened.bucket, .prefix = "catalog", .source_uri = "file:///fixture" };
            var seeded = try managed.create(a, "create", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);
            seeded.deinit(a);
        }
        probe.enabled = true;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        // Selection/cursor CPU scopes may replace the primary checkpoint;
        // SQL's additional authority check must survive that adaptation.
        var call_context = probe.context.withExecution(&execution);
        call_context.wait = probe.context.wait;
        const result = Operation.run(arena.allocator(), stage, &store, call_context, token, identity);
        if (mode == .success) try result else try std.testing.expectError(switch (mode) {
            .canceled => error.Canceled,
            .expired => error.DeadlineExceeded,
            .rejected_resume => error.SchedulerClosed,
            .forbidden => error.Forbidden,
            .success => unreachable,
        }, result);
        try std.testing.expect(probe.ends >= probe.reject_at);
        if (stage == .pin) {
            // A rejected grant after PUT may leave an expiring retention pin;
            // it must not turn into a published query result or lose the pin.
            const persisted: @import("../serverless/lake_snapshot_pins.zig").Store = .{ .client = store.opened.client, .bucket = store.opened.bucket, .prefix = "pins" };
            try std.testing.expect((try persisted.deadline(a, "42")) != 0);
        }
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

test "SQL prepared numeric serial owner publishes coordinator scope without parallel scope and charges foreground scratch" {
    const a = std.testing.allocator;
    const Prepared = @import("antfly_local_sources").sql_typed_kernel.PreparedNumeric;
    const Partitioned = @import("../common/allocation_budget.zig").Partitioned;
    const required = Prepared.scratch_bytes + Partitioned.overhead(.of(Prepared));
    var memory: Partitioned = .{ .foreground = .{ .child = a, .limit = required }, .control = .{ .child = a, .limit = 64 } };
    var server: http_server.ApiHttpServer = undefined;
    server.alloc = a;
    server.request_memory = &memory;
    server.embedding_provider_runtime.io = std.testing.io;
    var scheduler = try http_server.RunnableScheduler.Scheduler.init(.{ .slots = 2 });
    defer scheduler.close();
    var execution: http_server.RunnableScheduler.Execution = .{};
    try execution.start(&scheduler, .{ .io = std.testing.io }, .query);
    defer execution.finish();
    var identity: ?http_server.AuthenticatedIdentity = null;
    var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .runnable = &execution, .serial_execution = true };
    try adapter.beginReadFence();
    defer adapter.endReadFence();
    try std.testing.expect(adapter.cpuScope() == null);
    const scope = adapter.coordinatorScope().?;
    try std.testing.expect(scope.execution == &execution);
    try std.testing.expect(scope.check != null);
    try std.testing.expect(scope.check.?.context == @as(*anyopaque, @ptrCast(&adapter)));
    var compiled = try @import("antfly_local_sources").sql_compiler.compileScalar(a, "1 + 2", .{});
    defer compiled.deinit();
    var program = try @import("antfly_local_sources").sql_scalar.bind(a, compiled.expression, &.{}, &.{}, .{});
    defer program.deinit();
    const stage = (try Prepared.init(scope.allocator, &program)).?;
    var stage_live = true;
    defer if (stage_live) stage.deinit();
    try std.testing.expectEqual(required, memory.foreground.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), memory.control.stats().used_bytes);
    try std.testing.expectError(error.OutOfMemory, scope.allocator.alloc(u8, 1));
    const control = try memory.allocator(true).alloc(u8, 1);
    memory.allocator(true).free(control);
    stage.deinit();
    stage_live = false;
    try std.testing.expectEqual(@as(usize, 0), memory.foreground.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), memory.control.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), execution.entry.?.children);
    adapter.serial_execution = false;
    try std.testing.expect(adapter.cpuScope().?.execution == scope.execution);
}

test "SQL scalar lifetime callbacks do not yield allocate or validate catalog inside native frames" {
    const a = std.testing.allocator;
    const Source = struct {
        calls: usize = 0,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return error.UnexpectedScalarStatus;
        }
        fn catalogRead(raw: *anyopaque, _: std.mem.Allocator, _: operation.RequestContext, _: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.ScalarFrameAttemptedCatalogIo;
        }
    };
    const Signal = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    var source: Source = .{};
    var server: http_server.ApiHttpServer = undefined;
    server.alloc = a;
    server.source = .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.catalogRead, .supports_query_definitions = true } };
    var scheduler = try http_server.RunnableScheduler.Scheduler.init(.{ .slots = 2, .checkpoint_budget = 1 });
    defer scheduler.close();
    var canceled: std.atomic.Value(bool) = .init(false);
    const clock = @import("antfly_platform").time.monotonicNs;
    const deadline = clock() + 30 * std.time.ns_per_s;
    var execution: http_server.RunnableScheduler.Execution = .{};
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = clock, .deadline_ns = deadline, .retained_bytes = 17, .cancellation = .{ .ptr = &canceled, .requested = Signal.requested } }, .query);
    defer execution.finish();
    var identity: ?http_server.AuthenticatedIdentity = null;
    var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{ .deadline_ns = deadline, .cancellation = .fromAtomic(&canceled) }, .runnable = &execution };
    try adapter.beginReadFence();
    defer adapter.endReadFence();
    try adapter.read_guard.?.capture(.{ .database = "main", .namespace = "public", .name = "docs", .physical = "physical", .id = 1, .schema_version = 1, .catalog_revision = 7, .filter = null });
    const generation = execution.resume_generation;
    for (0..16) |_| try Adapter.scalarCheckpoint(&adapter);
    try std.testing.expectEqual(generation, execution.resume_generation);
    try std.testing.expectEqual(@as(usize, 0), source.calls);
    try std.testing.expect(adapter.read_guard.?.checked_generation == null);
    var unavailable = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    server.alloc = unavailable.allocator();
    try Adapter.scalarCheckpoint(&adapter);
    try std.testing.expectEqual(@as(usize, 0), unavailable.allocations);
    server.alloc = a;
    // The ordinary coordinator boundary still performs full read validation.
    try std.testing.expectError(error.ScalarFrameAttemptedCatalogIo, Adapter.checkpoint(&adapter));
    try std.testing.expectEqual(@as(usize, 1), source.calls);
    const after_coordinator = execution.resume_generation;
    canceled.store(true, .release);
    try std.testing.expectError(error.Canceled, Adapter.scalarCheckpoint(&adapter));
    canceled.store(false, .release);
    adapter.context.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, Adapter.scalarCheckpoint(&adapter));
    try std.testing.expectEqual(after_coordinator, execution.resume_generation);
    try std.testing.expectEqual(deadline, execution.entry.?.options.deadline_ns.?);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 17), scheduler.stats().retained_bytes);
}

test "SQL scalar lifetime callbacks inspect write execution without a foreground runnable" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    Clock.tick = 0;
    var scheduler = try http_server.RunnableScheduler.Scheduler.init(.{ .slots = 2, .checkpoint_budget = 1 });
    defer scheduler.close();
    var execution: http_server.RunnableScheduler.Execution = .{};
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100, .retained_bytes = 19 }, .write);
    defer execution.finish();
    var adapter: Adapter = .{ .server = undefined, .identity = undefined, .context = .{}, .write_execution = &execution };
    const generation = execution.resume_generation;
    try Adapter.scalarCheckpoint(&adapter);
    Clock.tick = 100;
    try std.testing.expectError(error.DeadlineExceeded, Adapter.scalarCheckpoint(&adapter));
    try std.testing.expectEqual(generation, execution.resume_generation);
    try std.testing.expectEqual(@as(u64, 100), execution.entry.?.options.deadline_ns.?);
    try std.testing.expectEqual(@as(u64, 19), scheduler.stats().retained_bytes);
}

test "SQL scalar lifetime callbacks preserve preauthorized cleanup after foreground cancellation and close" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    const Signal = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    Clock.tick = 0;
    var scheduler = try http_server.RunnableScheduler.Scheduler.init(.{ .slots = 3, .cleanup_slots = 1, .checkpoint_budget = 1 });
    defer scheduler.close();
    var canceled: std.atomic.Value(bool) = .init(false);
    var execution: http_server.RunnableScheduler.Execution = .{};
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 50, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = Signal.requested } }, .query);
    defer execution.finish();
    var adapter: Adapter = .{ .server = undefined, .identity = undefined, .context = .{ .deadline_ns = 0, .cancellation = .fromAtomic(&canceled) }, .runnable = &execution };
    adapter.cleanup_phase = try @import("runnable_local_cleanup.zig").Cleanup.init(&execution);
    try execution.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 100);
    canceled.store(true, .release);
    try execution.activateCleanup();
    scheduler.close();
    const generation = execution.resume_generation;
    try Adapter.scalarCheckpoint(&adapter);
    try std.testing.expectEqual(generation, execution.resume_generation);
    try std.testing.expectEqual(@as(u64, 100), execution.entry.?.options.deadline_ns.?);
    Clock.tick = 100;
    try std.testing.expectError(error.DeadlineExceeded, Adapter.scalarCheckpoint(&adapter));
    try std.testing.expectEqual(generation, execution.resume_generation);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
}

test "SQL CPU loan policy validation waits for ordinary CPU and retains original failures" {
    const native = http_server.RunnableScheduler;
    const io = std.testing.io;
    const Clock = struct {
        var tick: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return tick.load(.acquire);
        }
    };
    const Source = struct {
        execution: *native.Execution,
        calls: std.atomic.Value(usize) = .init(0),
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return error.UnexpectedSqlPolicyStatus;
        }
        fn catalogRead(raw: *anyopaque, _: std.mem.Allocator, _: operation.RequestContext, _: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .acq_rel);
            if (self.execution.entry.?.borrowed_read_cpu or self.execution.entry.?.cpu_borrow_budget != null) return error.InvalidRunnableTransition;
            return error.Forbidden;
        }
    };
    const Signal = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    const Worker = struct {
        fn run(adapter: *Adapter) !void {
            try Adapter.checkpoint(adapter);
        }
    };
    const Mode = enum { catalog_refusal, canceled, expired, closed };
    inline for (.{ Mode.catalog_refusal, Mode.canceled, Mode.expired, Mode.closed }) |mode| {
        Clock.tick.store(0, .release);
        var scheduler = try native.Scheduler.init(.{ .slots = 5, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = 2, .max_wait_ms = 5000 });
        defer scheduler.close();
        var base: native.Entry = .{ .options = .{ .io = io }, .class = .background, .bytes = 11 };
        try scheduler.submit(&base);
        defer scheduler.finish(&base);
        var execution: native.Execution = .{};
        defer execution.finish();
        var source: Source = .{ .execution = &execution };
        var server: http_server.ApiHttpServer = undefined;
        server.alloc = std.testing.allocator;
        server.source = .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.catalogRead, .supports_query_definitions = true } };
        var canceled: std.atomic.Value(bool) = .init(false);
        var identity: ?http_server.AuthenticatedIdentity = null;
        var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{ .cancellation = .fromAtomic(&canceled) }, .runnable = &execution };
        try adapter.beginReadFence();
        defer adapter.endReadFence();
        try adapter.read_guard.?.capture(.{ .database = "main", .namespace = "public", .name = "docs", .physical = "physical", .id = 1, .schema_version = 1, .catalog_revision = 7, .filter = null });
        try execution.startBorrowable(&scheduler, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 100, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = Signal.requested } }, .query, .{ .max_non_yield_ns = 1000 });
        try std.testing.expectEqual(@as(usize, 1), scheduler.cpuBorrowStats().active);
        var floor: native.Entry = .{ .options = .{ .io = io }, .class = .query, .read_lane = .bounded, .bytes = 7 };
        try scheduler.submit(&floor);
        defer scheduler.finish(&floor);
        try std.testing.expectEqual(native.State.running, scheduler.state(&floor));
        var future = try io.concurrent(Worker.run, .{&adapter});
        defer future.cancel(io) catch {};
        const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
        while (scheduler.state(&execution.entry.?) != .queued) {
            if (source.calls.load(.acquire) != 0) return future.await(io);
            if (scheduler.state(&execution.entry.?) == .stopped) return future.await(io);
            if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.SqlPolicyDidNotReturnCpuLoan;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expectEqual(@as(usize, 0), source.calls.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), scheduler.cpuBorrowStats().active);
        try std.testing.expectEqual(native.State.running, scheduler.state(&floor));
        try std.testing.expectEqual(@as(u64, 41), scheduler.stats().retained_bytes);
        switch (mode) {
            .catalog_refusal => scheduler.finish(&base),
            .canceled => canceled.store(true, .release),
            .expired => Clock.tick.store(100, .release),
            .closed => scheduler.close(),
        }
        const expected = switch (mode) {
            .catalog_refusal => error.Forbidden,
            .canceled => error.Canceled,
            .expired => error.DeadlineExceeded,
            .closed => error.SchedulerClosed,
        };
        try std.testing.expectError(expected, future.await(io));
        try std.testing.expectEqual(@as(usize, if (mode == .catalog_refusal) 1 else 0), source.calls.load(.acquire));
        try std.testing.expectEqual(@as(?u64, null), adapter.read_guard.?.checked_generation);
        try std.testing.expectEqual(@as(u64, 100), execution.entry.?.options.deadline_ns.?);
        try std.testing.expectEqual(@as(?native.CpuBorrowBudget, null), execution.entry.?.cpu_borrow_budget);
        scheduler.finish(&base);
        scheduler.finish(&floor);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "SQL CPU loan policy validation exposes checked stage transitions without retiring ingress" {
    const runtime = @import("antfly_runtime_abi");
    const native = http_server.RunnableScheduler;
    const io = std.testing.io;
    const Worker = struct {
        fn run(borrow: runtime.runnable_abi.Borrowed) !void {
            try borrow.setCpuBorrow(&.{ .enabled = 0 });
        }
    };
    var owner = try native.Scheduler.init(.{ .slots = 5, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = 2, .checkpoint_budget = 1, .max_wait_ms = 5000 });
    defer owner.close();
    var execution: native.Execution = .{};
    defer execution.finish();
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    try execution.start(&owner, .{ .io = io, .native_now_ns = @import("antfly_platform").time.monotonicNs, .deadline_ns = deadline, .retained_bytes = 23 }, .query);
    var server: http_server.ApiHttpServer = undefined;
    server.alloc = std.testing.allocator;
    var identity: ?http_server.AuthenticatedIdentity = null;
    var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .runnable = &execution };
    const borrow = adapter.providerRunnable().?;
    try std.testing.expectError(error.InvalidReadFenceTransition, borrow.setCpuBorrow(&.{ .enabled = 1, .max_non_yield_ns = 1000 }));
    try adapter.beginReadFence();
    defer adapter.endReadFence();
    var malformed: runtime.runnable_abi.CpuBorrow = .{ .enabled = 1, .max_non_yield_ns = 1000 };
    malformed.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, borrow.setCpuBorrow(&malformed));
    malformed = .{ .enabled = 2, .max_non_yield_ns = 1000 };
    try std.testing.expectError(error.InvalidArgument, borrow.setCpuBorrow(&malformed));
    try std.testing.expectEqual(@as(?native.CpuBorrowBudget, null), execution.entry.?.cpu_borrow_budget);
    const generation = execution.resume_generation;
    try borrow.setCpuBorrow(&.{ .enabled = 1, .max_non_yield_ns = 1000 });
    try std.testing.expectEqual(generation, execution.resume_generation);
    var peer: native.Entry = .{ .options = .{ .io = io }, .class = .background, .cpu_borrow_budget = .{ .max_non_yield_ns = 1000 } };
    try owner.submit(&peer);
    defer owner.finish(&peer);
    try std.testing.expect(borrow.checkpoint(borrow.context).isOk());
    try std.testing.expect(execution.entry.?.borrowed_read_cpu);
    var floor: native.Entry = .{ .options = .{ .io = io }, .class = .query, .read_lane = .bounded };
    try owner.submit(&floor);
    defer owner.finish(&floor);
    try std.testing.expectEqual(native.State.running, owner.state(&floor));
    var future = try io.concurrent(Worker.run, .{borrow});
    defer future.cancel(io) catch {};
    const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
    while (owner.state(&execution.entry.?) != .queued) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.SqlCpuBorrowDisableDidNotWait;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(native.State.running, owner.state(&floor));
    try std.testing.expectEqual(@as(usize, 0), owner.cpuBorrowStats().active);
    owner.finish(&peer);
    try future.await(io);
    try std.testing.expectEqual(@as(?native.CpuBorrowBudget, null), execution.entry.?.cpu_borrow_budget);
    try std.testing.expectEqual(deadline, execution.entry.?.options.deadline_ns.?);
    owner.finish(&floor);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), owner.stats().retained_bytes);
    // Ending a stage must clear eligibility even when the foreground SQL
    // context has expired independently of the retained execution deadline.
    try borrow.setCpuBorrow(&.{ .enabled = 1, .max_non_yield_ns = 1000 });
    adapter.context.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, borrow.setCpuBorrow(&.{ .enabled = 0 }));
    try std.testing.expectEqual(@as(?native.CpuBorrowBudget, null), execution.entry.?.cpu_borrow_budget);
    try std.testing.expectEqual(deadline, execution.entry.?.options.deadline_ns.?);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
}

test "SQL CPU loan policy validation resumes captured numeric work under unchanged and revoked authority" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const local = @import("antfly_local_sources");
    const native = http_server.RunnableScheduler;
    const Prepared = local.sql_typed_kernel.PreparedNumeric;
    const Partitioned = @import("../common/allocation_budget.zig").Partitioned;
    const users_api = @import("../usermgr/mod.zig");
    const casbin = @import("antfly_casbin");
    const Mode = enum { success, cache_busy, catalog_changed, policy_changed, credential_revoked, policy_expanded };
    const Clock = struct {
        fn now() u64 {
            return 0;
        }
    };
    const Source = struct {
        execution: *native.Execution,
        resolves: std.atomic.Value(usize) = .init(0),
        policies: std.atomic.Value(usize) = .init(0),
        revision: std.atomic.Value(u64) = .init(19),
        generation: std.atomic.Value(u64) = .init(2),
        const schema_json =
            \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"},"tenant":{"type":"keyword"}},"additionalProperties":false}}}}
        ;
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn resolve(raw: *anyopaque, alloc: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.execution.entry.?.borrowed_read_cpu or self.execution.entry.?.cpu_borrow_budget != null) return error.InvalidRunnableTransition;
            if (call == .policy_publication_status) {
                try std.testing.expect(context.row_policy_install_authority);
                _ = self.policies.fetchAdd(1, .acq_rel);
                const generation = self.generation.load(.acquire);
                const stamp: local.system_catalog_policies.PublicationStamp = .{ .table_id = 7, .schema_version = 1, .generation = generation, .catalog_epoch = if (generation == 2) 3 else 4, .phase = .active };
                return std.json.Stringify.valueAlloc(alloc, stamp, .{});
            }
            if (call != .resolve_many) return error.TestUnexpectedCatalogCall;
            _ = self.resolves.fetchAdd(1, .acq_rel);
            try std.testing.expectEqual(@as(usize, 1), call.resolve_many.targets.len);
            const tables = [_]?system_catalog.ResolvedTable{.{ .table_id = 7, .name = "private-7", .query_definition = .{ .table_id = 7, .schema_json = schema_json, .read_schema_json = "", .indexes_json = "{}" } }};
            return std.json.Stringify.valueAlloc(alloc, system_catalog.ResolvedMany{ .revision = self.revision.load(.acquire), .tables = &tables }, .{});
        }
    };
    const Input = struct {
        count: usize = 64,
        calls: *usize,
        ordinal: u32,
        pub fn cell(self: @This(), _: std.mem.Allocator, _: usize, ordinal: u32) !local.sql_scalar.Datum {
            if (ordinal != self.ordinal) return error.InvalidSqlBackendResponse;
            self.calls.* += 1;
            return local.sql_scalar.Datum.json(.{ .integer = 41 });
        }
    };
    const Worker = struct {
        fn run(borrow: @import("antfly_runtime_abi").runnable_abi.Borrowed) !void {
            try borrow.setCpuBorrow(&.{ .enabled = 0 });
        }
    };
    inline for (.{ Mode.success, Mode.cache_busy, Mode.catalog_changed, Mode.policy_changed, Mode.credential_revoked, Mode.policy_expanded }) |mode| {
        var store = users_api.MemoryStore.init(a);
        defer store.deinit();
        var policies = casbin.MemoryAdapter.init(a);
        defer policies.deinit();
        var users = try users_api.UserManager.init(a, store.iface(), try users_api.initDefaultEnforcer(a, policies.iface()));
        defer users.deinit();
        var permission = try users_api.Permission.initOwned(a, .table, "*", .read);
        defer permission.deinit(a);
        var user = try users.createUserWithMetadata("alice", "secret", &.{permission}, "{\"tenant\":\"initial\"}");
        defer user.deinit(a);
        try users.setRowFilter("alice", "docs", "{\"term\":{\"tenant\":\"initial\"}}");
        const principal = try a.dupe(u8, "basic:alice");
        defer a.free(principal);
        var snapshot = try users.snapshotCredentialPolicies(principal);
        defer snapshot.deinit(a);
        var identity = try http_server.cloneCatalogIdentity(a, .{ .user_instance_id = snapshot.instance_id, .username = snapshot.policy.username, .credential_principal = principal, .permissions = snapshot.policy.permissions, .row_filter = snapshot.policy.row_filter, .metadata_json = snapshot.policy.metadata_json, .roles = snapshot.policy.roles, .live_user_manager = &users });
        defer identity.?.deinit(a);
        var scheduler = try native.Scheduler.init(.{ .slots = 5, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = 2, .max_wait_ms = 5000 });
        defer scheduler.close();
        var execution: native.Execution = .{};
        defer execution.finish();
        try execution.start(&scheduler, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 1000000, .retained_bytes = 23 }, .query);
        var source: Source = .{ .execution = &execution };
        var server = http_server.ApiHttpServer.init(a, .{ .auth_enabled = true, .user_manager = &users, .imported_runtime_io = .{ .api = io } }, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.resolve, .supports_query_definitions = true } }, null, null);
        defer server.deinit();
        var forced_cache_busy = false;
        defer if (forced_cache_busy) {
            server.sql_schema_cache.builds = 0;
            for (server.sql_schema_cache.slots[0..4]) |*slot| slot.building = false;
        };
        try std.testing.expect(server.request_memory == null);
        const memory = try a.create(Partitioned);
        const scratch_bytes = Prepared.scratch_bytes + Partitioned.overhead(.of(Prepared));
        const policy_bytes = 64 << 10;
        const policy_charge = policy_bytes + Partitioned.overhead(.of(u8));
        memory.* = .{ .foreground = .{ .child = a, .limit = scratch_bytes + policy_charge }, .control = .{ .child = a, .limit = 64 } };
        server.request_memory = memory;
        var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .runnable = &execution, .serial_execution = true };
        try adapter.beginReadFence();
        defer adapter.endReadFence();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const table = try Adapter.resolve(&adapter, arena.allocator(), .{ .table = "docs" }, .read);
        try std.testing.expectEqual(@as(u64, 7), table.id);
        try std.testing.expectEqual(@as(u64, 2), adapter.read_guard.?.tables.items[0].row_policy.?.generation);
        const scope = adapter.coordinatorScope().?;
        try std.testing.expect(adapter.cpuScope() == null);
        const scalar_columns = try arena.allocator().alloc(local.sql_scalar.Column, table.columns.len);
        for (table.columns, scalar_columns) |column, *out| out.* = .{ .name = column.name, .type = column.type, .element_type = column.element_type };
        const ordinal: u32 = for (scalar_columns, 0..) |column, index| {
            if (std.mem.eql(u8, column.name, "n")) break @intCast(index);
        } else return error.MissingNumericColumn;
        var compiled = try local.sql_compiler.compileScalar(a, "n + 1", .{});
        defer compiled.deinit();
        var program = try local.sql_scalar.bind(a, compiled.expression, scalar_columns, &.{}, .{});
        defer program.deinit();
        const stage = (try Prepared.init(scope.allocator, &program)).?;
        var stage_live = true;
        defer if (stage_live) stage.deinit();
        var input_calls: usize = 0;
        try stage.prepare(Input{ .calls = &input_calls, .ordinal = ordinal }, &.{});
        while (stage.next_instruction < program.root) try std.testing.expect((try stage.step()) == null);
        try std.testing.expectEqual(.binary, std.meta.activeTag(stage.instructions[program.root].operation));
        try std.testing.expectEqual(scratch_bytes, memory.foreground.stats().used_bytes);
        const borrow = adapter.providerRunnable().?;
        try std.testing.expectError(error.InvalidRunnableTransition, borrow.setCpuBorrow(&.{ .enabled = 1, .max_non_yield_ns = 1000 }));
        // Actual validation must succeed from the reserved region even while
        // its foreground parent is completely occupied. No separate catalog
        // allocator may hide an allocation escape in this schedule.
        server.alloc = memory.allocator(false);
        defer server.alloc = a;
        try std.testing.expectError(error.OutOfMemory, adapter.reserveReadValidationMemory(1));
        try std.testing.expect(adapter.read_policy_memory == null);
        try std.testing.expectEqual(scratch_bytes, memory.foreground.stats().used_bytes);
        try std.testing.expectEqual(@as(?native.CpuBorrowBudget, null), execution.entry.?.cpu_borrow_budget);
        try adapter.reserveReadValidationMemory(policy_bytes);
        try std.testing.expectEqual(scratch_bytes + policy_charge, memory.foreground.stats().used_bytes);
        try std.testing.expectError(error.OutOfMemory, scope.allocator.alloc(u8, 1));
        // The former unreserved validation path cannot allocate at this heap
        // pressure. The reservation must complete the same real validation.
        try std.testing.expectError(error.OutOfMemory, adapter.read_guard.?.check(server.alloc, execution.resume_generation, true, .{ .ptr = &adapter, .check = Adapter.validateReadPins }));
        try adapter.validateReadFence(true);
        const control = try memory.allocator(true).alloc(u8, 1);
        defer memory.allocator(true).free(control);
        const resolves = source.resolves.load(.acquire);
        const policy_calls = source.policies.load(.acquire);
        const generation = execution.resume_generation;
        var peer: native.Entry = .{ .options = .{ .io = io }, .class = .write, .bytes = 17 };
        try scheduler.submit(&peer);
        defer scheduler.finish(&peer);
        try std.testing.expectEqual(native.State.queued, scheduler.state(&peer));
        try borrow.setCpuBorrow(&.{ .enabled = 1, .max_non_yield_ns = 1000 });
        try std.testing.expect(execution.entry.?.borrowed_read_cpu);
        try std.testing.expectEqual(native.State.running, scheduler.state(&peer));
        try std.testing.expectEqual(generation, execution.resume_generation);
        try std.testing.expectEqual(resolves, source.resolves.load(.acquire));
        try std.testing.expectEqual(policy_calls, source.policies.load(.acquire));
        var floor: native.Entry = .{ .options = .{ .io = io }, .class = .query, .read_lane = .bounded };
        try scheduler.submit(&floor);
        defer scheduler.finish(&floor);
        try std.testing.expectEqual(native.State.running, scheduler.state(&floor));
        // The actual addition computes private intermediate values on the same
        // validated grant. It neither publishes output nor crosses an IO edge.
        try std.testing.expect((try stage.step()) == null);
        try std.testing.expectEqual(@as(usize, 64), input_calls);
        try std.testing.expectEqual(scratch_bytes + policy_charge, memory.foreground.stats().used_bytes);
        var future = try io.concurrent(Worker.run, .{borrow});
        defer future.cancel(io) catch {};
        const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
        while (scheduler.state(&execution.entry.?) != .queued) {
            if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.CapturedStageDidNotReturnLoan;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expectEqual(resolves, source.resolves.load(.acquire));
        try std.testing.expectEqual(policy_calls, source.policies.load(.acquire));
        try std.testing.expectEqual(native.State.running, scheduler.state(&floor));
        switch (mode) {
            .success => {},
            .cache_busy => {
                // Force the real uncached derivation path after warm preflight.
                // These counters model four occupied builder slots; this is not
                // an assertion about four independently running cache workers.
                server.sql_schema_cache.deinit();
                server.sql_schema_cache = .init(a);
                server.sql_schema_cache.builds = 4;
                for (server.sql_schema_cache.slots[0..4]) |*slot| slot.building = true;
                forced_cache_busy = true;
            },
            .catalog_changed => source.revision.store(20, .release),
            .policy_changed => source.generation.store(3, .release),
            .credential_revoked => try users.removePermissionFromUser("alice", "*", .table),
            .policy_expanded => {
                // Live policy can grow after preflight. Its owned snapshot must
                // refuse within the existing reservation, with no heap fallback.
                const expanded = try a.alloc(u8, policy_bytes * 2);
                defer a.free(expanded);
                @memset(expanded, 'x');
                const filter = try std.fmt.allocPrint(a, "{{\"term\":{{\"tenant\":\"{s}\"}}}}", .{expanded});
                defer a.free(filter);
                try users.setRowFilter("alice", "docs", filter);
            },
        }
        scheduler.finish(&peer);
        if (mode == .success or mode == .cache_busy) {
            try future.await(io);
            const output = (try stage.step()).?;
            for (output) |value| try std.testing.expectEqual(@as(i64, 42), value.value.integer);
            try std.testing.expectEqual(execution.resume_generation, adapter.read_guard.?.checked_generation.?);
        } else try std.testing.expectError(switch (mode) {
            .catalog_changed => error.CatalogGenerationChanged,
            .policy_changed => error.RowPolicyCatalogChanged,
            .credential_revoked => error.Forbidden,
            .policy_expanded => error.OutOfMemory,
            else => unreachable,
        }, future.await(io));
        if (mode == .policy_expanded) {
            // A failed revalidation cannot cache authority or retain an active
            // arena. The root and reservation stay owned until explicit finish.
            try std.testing.expect(adapter.read_guard.?.checked_generation == null);
            try std.testing.expect(!adapter.read_guard.?.validating);
            try std.testing.expect(!adapter.read_policy_memory.?.active);
            try std.testing.expectEqual(scratch_bytes + policy_charge, memory.foreground.stats().used_bytes);
            const control_after_refusal = try memory.allocator(true).alloc(u8, 1);
            memory.allocator(true).free(control_after_refusal);
            try users.setRowFilter("alice", "docs", "{\"term\":{\"tenant\":\"initial\"}}");
            // This only checks bounded arena reuse after failure. The failed
            // operation's private numeric result is never exposed.
            try adapter.validateReadFence(true);
            try std.testing.expect(!adapter.read_policy_memory.?.active);
            try std.testing.expectEqual(scratch_bytes + policy_charge, memory.foreground.stats().used_bytes);
        }
        try std.testing.expect(source.resolves.load(.acquire) > resolves);
        try std.testing.expectEqual(@as(usize, 0), scheduler.cpuBorrowStats().active);
        try std.testing.expectEqual(@as(?native.CpuBorrowBudget, null), execution.entry.?.cpu_borrow_budget);
        try std.testing.expectEqual(@as(u64, 1000000), execution.entry.?.options.deadline_ns.?);
        scheduler.finish(&floor);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        stage.deinit();
        stage_live = false;
        try std.testing.expectEqual(policy_charge, memory.foreground.stats().used_bytes);
        adapter.endReadFence();
        try std.testing.expectEqual(@as(usize, 0), memory.foreground.stats().used_bytes);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    }
}

test "SQL CPU loan policy validation preflight never publishes reservations after original lifetime stops" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const native = http_server.RunnableScheduler;
    const Partitioned = @import("../common/allocation_budget.zig").Partitioned;
    const Mode = enum { expired, canceled, closed };
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    const Signal = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    const Source = struct {
        mode: Mode,
        scheduler: *native.Scheduler,
        canceled: *bool,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn resolve(raw: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (call == .policy_publication_status) {
                const result = try alloc.dupe(u8, "null");
                switch (self.mode) {
                    .expired => Clock.tick = 100,
                    .canceled => self.canceled.* = true,
                    .closed => self.scheduler.close(),
                }
                return result;
            }
            if (call != .resolve_many) return error.TestUnexpectedCatalogCall;
            const schema_json =
                \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"}},"additionalProperties":false}}}}
            ;
            const tables = [_]?system_catalog.ResolvedTable{.{ .table_id = 7, .name = "private-7", .query_definition = .{ .table_id = 7, .schema_json = schema_json, .read_schema_json = "", .indexes_json = "{}" } }};
            return std.json.Stringify.valueAlloc(alloc, system_catalog.ResolvedMany{ .revision = 19, .tables = &tables }, .{});
        }
    };
    inline for (.{ Mode.expired, Mode.canceled, Mode.closed }) |mode| {
        Clock.tick = 0;
        var canceled = false;
        var scheduler = try native.Scheduler.init(.{ .slots = 2 });
        defer scheduler.close();
        var execution: native.Execution = .{};
        defer execution.finish();
        try execution.start(&scheduler, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 100, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = Signal.requested } }, .query);
        var source: Source = .{ .mode = mode, .scheduler = &scheduler, .canceled = &canceled };
        var server = http_server.ApiHttpServer.init(a, .{ .imported_runtime_io = .{ .api = io } }, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.resolve } }, null, null);
        defer server.deinit();
        const memory = try a.create(Partitioned);
        const bytes = 64 << 10;
        memory.* = .{ .foreground = .{ .child = a, .limit = bytes + Partitioned.overhead(.of(u8)) }, .control = .{ .child = a, .limit = 64 } };
        server.request_memory = memory;
        var identity: ?http_server.AuthenticatedIdentity = null;
        // The SQL context intentionally has independent clocks/tokens. The
        // original execution must still fence the post-validation handoff.
        var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .runnable = &execution };
        try adapter.beginReadFence();
        defer adapter.endReadFence();
        try adapter.read_guard.?.capture(.{ .database = "default", .namespace = "public", .name = "docs", .physical = "private-7", .id = 7, .schema_version = 1, .catalog_revision = 19, .filter = null });
        const expected = switch (mode) {
            .expired => error.DeadlineExceeded,
            .canceled => error.Canceled,
            .closed => error.SchedulerClosed,
        };
        try std.testing.expectError(expected, adapter.reserveReadValidationMemory(bytes));
        try std.testing.expect(adapter.read_policy_memory == null);
        try std.testing.expectEqual(@as(usize, 0), memory.foreground.stats().used_bytes);
        try std.testing.expectEqual(@as(usize, 0), memory.control.stats().used_bytes);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        try std.testing.expectEqual(@as(u64, 100), execution.entry.?.options.deadline_ns.?);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    }
}

test "SQL CPU loan policy validation fences original lifetime after metadata and cached checks" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const native = http_server.RunnableScheduler;
    const Partitioned = @import("../common/allocation_budget.zig").Partitioned;
    const Mode = enum { expired, canceled, closed };
    const Phase = enum { forced, cached, cpu_return };
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    const Signal = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    const Source = struct {
        mode: Mode,
        scheduler: *native.Scheduler,
        canceled: *bool,
        stop_on_policy: bool = false,
        fn stop(self: *@This()) void {
            switch (self.mode) {
                .expired => Clock.tick = 100,
                .canceled => self.canceled.* = true,
                .closed => self.scheduler.close(),
            }
        }
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn resolve(raw: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (call == .policy_publication_status) {
                const result = try alloc.dupe(u8, "null");
                if (self.stop_on_policy) self.stop();
                return result;
            }
            if (call != .resolve_many) return error.TestUnexpectedCatalogCall;
            const schema_json =
                \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"}},"additionalProperties":false}}}}
            ;
            const tables = [_]?system_catalog.ResolvedTable{.{ .table_id = 7, .name = "private-7", .query_definition = .{ .table_id = 7, .schema_json = schema_json, .read_schema_json = "", .indexes_json = "{}" } }};
            return std.json.Stringify.valueAlloc(alloc, system_catalog.ResolvedMany{ .revision = 19, .tables = &tables }, .{});
        }
    };
    inline for (.{ Phase.forced, Phase.cached, Phase.cpu_return }) |phase| {
        inline for (.{ Mode.expired, Mode.canceled, Mode.closed }) |mode| {
            Clock.tick = 0;
            var canceled = false;
            var scheduler = try native.Scheduler.init(.{ .slots = 2 });
            defer scheduler.close();
            var execution: native.Execution = .{};
            defer execution.finish();
            try execution.start(&scheduler, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 100, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = Signal.requested } }, .query);
            var source: Source = .{ .mode = mode, .scheduler = &scheduler, .canceled = &canceled };
            var server = http_server.ApiHttpServer.init(a, .{ .imported_runtime_io = .{ .api = io } }, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.resolve } }, null, null);
            defer server.deinit();
            const memory = try a.create(Partitioned);
            const bytes = 64 << 10;
            memory.* = .{ .foreground = .{ .child = a, .limit = bytes + Partitioned.overhead(.of(u8)) }, .control = .{ .child = a, .limit = 64 } };
            server.request_memory = memory;
            var identity: ?http_server.AuthenticatedIdentity = null;
            // The SQL context intentionally has independent clocks/tokens. The
            // original execution must still fence the post-validation handoff.
            var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .runnable = &execution };
            try adapter.beginReadFence();
            defer adapter.endReadFence();
            try adapter.read_guard.?.capture(.{ .database = "default", .namespace = "public", .name = "docs", .physical = "private-7", .id = 7, .schema_version = 1, .catalog_revision = 19, .filter = null });
            const expected = switch (mode) {
                .expired => error.DeadlineExceeded,
                .canceled => error.Canceled,
                .closed => error.SchedulerClosed,
            };
            try adapter.reserveReadValidationMemory(bytes);
            const charge = bytes + Partitioned.overhead(.of(u8));
            try std.testing.expectEqual(charge, memory.foreground.stats().used_bytes);
            const borrow = adapter.providerRunnable().?;
            if (phase == .cpu_return) try borrow.setCpuBorrow(&.{ .enabled = 1, .max_non_yield_ns = 1000 });
            if (phase == .cached) source.stop() else source.stop_on_policy = true;
            const outcome = if (phase == .cpu_return)
                borrow.setCpuBorrow(&.{ .enabled = 0 })
            else
                adapter.validateReadFence(phase == .forced);
            try std.testing.expectError(expected, outcome);
            try std.testing.expect(adapter.read_guard.?.checked_generation == null);
            try std.testing.expect(!adapter.read_guard.?.validating);
            try std.testing.expect(!adapter.read_policy_memory.?.active);
            try std.testing.expect(execution.entry.?.cpu_borrow_budget == null);
            try std.testing.expectEqual(charge, memory.foreground.stats().used_bytes);
            adapter.endReadFence();
            try std.testing.expectEqual(@as(usize, 0), memory.foreground.stats().used_bytes);
            try std.testing.expectEqual(@as(usize, 0), memory.control.stats().used_bytes);
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
            try std.testing.expectEqual(@as(u64, 100), execution.entry.?.options.deadline_ns.?);
            execution.finish();
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        }
    }
}

test "SQL prepared numeric serial owner bundles scratch with original CPU and preserves fallback and cancellation" {
    const a = std.testing.allocator;
    const Budget = @import("../common/allocation_budget.zig");
    const Prepared = @import("antfly_local_sources").sql_typed_kernel.PreparedNumeric;
    const Mode = enum { success, busy, denied, unsupported, canceled, expired, metadata_oom, backing_oom, partition_denied, missing_cap };
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    const Node = struct {
        mode: Mode,
        used: usize = 0,
        peak: usize = 0,
        attempts: usize = 0,
        signal: *std.atomic.Value(bool),
        fn reserve(raw: *anyopaque, bytes: usize) callconv(.c) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.used += bytes;
            self.peak = @max(self.peak, self.used);
            return true;
        }
        fn attempt(raw: *anyopaque, bytes: usize) callconv(.c) Budget.Accounting.TryReserve {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.attempts += 1;
            switch (self.mode) {
                .busy => return .busy,
                .denied => return .denied,
                .unsupported => return .unsupported,
                .canceled => self.signal.store(true, .release),
                .expired => Clock.tick = 100,
                .success, .metadata_oom, .backing_oom, .partition_denied, .missing_cap => {},
            }
            _ = reserve(raw, bytes);
            return .reserved;
        }
        fn release(raw: *anyopaque, bytes: usize) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.used -= bytes;
        }
        fn canceled(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    var compiled = try @import("antfly_local_sources").sql_compiler.compileScalar(a, "n * 2 + 1", .{});
    defer compiled.deinit();
    const columns = [_]@import("antfly_local_sources").sql_scalar.Column{.{ .name = "n", .type = .integer }};
    var program = try @import("antfly_local_sources").sql_scalar.bind(a, compiled.expression, &columns, &.{}, .{});
    defer program.deinit();
    const page: catalog.ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .row_refs = &.{.{ .relational_key = "r" }}, .columns = &.{.{ .name = "n", .values = .{ .i64 = &.{7} } }} }, .selection = &.{0} };
    for (std.enums.values(Mode)) |mode| {
        Clock.tick = 0;
        var signal: std.atomic.Value(bool) = .init(false);
        var node: Node = .{ .mode = mode, .signal = &signal };
        var backing = std.testing.FailingAllocator.init(a, .{ .fail_index = if (mode == .metadata_oom) 0 else if (mode == .backing_oom) 1 else std.math.maxInt(usize) });
        const metadata = @sizeOf(Adapter.NumericScratch) + Budget.Partitioned.overhead(.of(Adapter.NumericScratch));
        var memory: Budget.Partitioned = .{ .foreground = .{ .child = backing.allocator(), .limit = if (mode == .partition_denied) metadata else 1 << 20, .accounting = .{ .ptr = &node, .reserve_fn = Node.reserve, .try_reserve_fn = if (mode == .missing_cap) null else Node.attempt, .release_fn = Node.release } }, .control = .{ .child = a, .limit = 64 } };
        var server: http_server.ApiHttpServer = undefined;
        server.alloc = a;
        server.request_memory = &memory;
        server.embedding_provider_runtime.io = std.testing.io;
        var scheduler = try http_server.RunnableScheduler.Scheduler.init(.{ .slots = 2 });
        defer scheduler.close();
        var execution: http_server.RunnableScheduler.Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100, .retained_bytes = 72, .cancellation = .{ .ptr = &signal, .requested = Node.canceled } }, .query);
        defer execution.finish();
        var identity: ?http_server.AuthenticatedIdentity = null;
        var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .runnable = &execution, .serial_execution = true };
        try adapter.beginReadFence();
        defer adapter.endReadFence();
        const grants = scheduler.stats().grants[@backingInt(http_server.RunnableScheduler.Class.query)];
        const evaluated = @import("antfly_local_sources").sql_vector_eval.evaluateColumnsWithScopes(a, &program, page, &columns, &.{}, null, null, adapter.coordinatorScope());
        if (mode == .canceled or mode == .expired) {
            try std.testing.expectError(if (mode == .canceled) error.Canceled else error.DeadlineExceeded, evaluated);
        } else {
            const values = (try evaluated).?;
            defer a.free(values);
            try std.testing.expectEqual(@as(i64, 15), values[0].value.integer);
            try std.testing.expectEqual(grants + @as(u64, if (mode == .metadata_oom) 0 else 1), scheduler.stats().grants[@backingInt(http_server.RunnableScheduler.Class.query)]);
        }
        const attempted: usize = if (mode == .metadata_oom or mode == .partition_denied or mode == .missing_cap) 0 else 1;
        const reserved = mode == .success or mode == .canceled or mode == .expired or mode == .backing_oom;
        // Metadata accounting precedes the physical allocation attempt,
        // including a backing OOM which rolls that charge back.
        const expected_peak = metadata + @as(usize, if (reserved) Prepared.scratch_bytes else 0);
        try std.testing.expectEqual(expected_peak, node.peak);
        try std.testing.expectEqual(attempted, node.attempts);
        try std.testing.expectEqual(backing.allocated_bytes, backing.freed_bytes);
        try std.testing.expectEqual(@as(usize, 0), node.used);
        try std.testing.expectEqual(@as(usize, 0), memory.foreground.stats().used_bytes);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 72), scheduler.stats().retained_bytes);
        try std.testing.expectEqual(@as(?u64, 100), execution.entry.?.options.deadline_ns);
        try std.testing.expectEqual(@as(usize, 0), execution.entry.?.children);
        try std.testing.expect(execution.entry.?.pending_resources == null);
        // Dispatch after retirement must never call the temporary ticket again.
        scheduler.notifyResourceProgress();
        try std.testing.expectEqual(attempted, node.attempts);
    }
}

test "SQL grouped coordinator scratch preserves exact batches and row fallback under original authority" {
    const local = @import("antfly_local_sources");
    const Budget = @import("../common/allocation_budget.zig");
    const Groups = local.sql_operators.Grouped;
    const Datum = local.sql_scalar.Datum;
    const Mode = enum { success, denied, backing_oom, canceled, expired, hash_canceled, estimate_canceled };
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    const count = 64;
    const scratch_bytes = comptime Groups.batchScratchBytes(count, false).?;
    const metadata = @sizeOf(Adapter.NumericScratch) + Budget.Partitioned.overhead(.of(Adapter.NumericScratch));
    const Node = struct {
        mode: Mode,
        signal: *std.atomic.Value(bool),
        backing: *std.testing.FailingAllocator,
        metadata_bytes: usize,
        used: usize = 0,
        peak: usize = 0,
        attempts: usize = 0,
        physical_retirement_checked: bool = false,
        fn reserve(raw: *anyopaque, bytes: usize) callconv(.c) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.used += bytes;
            self.peak = @max(self.peak, self.used);
            return true;
        }
        fn attempt(raw: *anyopaque, bytes: usize) callconv(.c) Budget.Accounting.TryReserve {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(bytes == scratch_bytes);
            self.attempts += 1;
            switch (self.mode) {
                .denied => return .denied,
                .canceled => self.signal.store(true, .release),
                .expired => Clock.tick = 100,
                else => {},
            }
            _ = reserve(raw, bytes);
            return .reserved;
        }
        fn release(raw: *anyopaque, bytes: usize) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (bytes == scratch_bytes) {
                // Only the ticket's metadata may remain physically allocated
                // when the fixed vector reservation is returned to the node.
                std.debug.assert(self.backing.allocated_bytes - self.backing.freed_bytes == self.metadata_bytes);
                self.physical_retirement_checked = true;
            }
            self.used -= bytes;
        }
        fn canceled(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    const a = std.testing.allocator;
    const specs = [_]local.sql_operators.AggregateSpec{ .{ .kind = .count, .input_type = .integer }, .{ .kind = .sum, .input_type = .integer } };
    const key = Datum.json(.{ .integer = 7 });
    const value = Datum.json(.{ .integer = 9007199254740993 });
    const keys: [count]Datum = @splat(key);
    const values: [count]Datum = @splat(value);
    const indices: [count]u32 = @splat(0);
    const Batch = local.sql_execution_batch.Batch;
    const encoded_key: Batch = .{ .dictionary = .{ .values = &.{key}, .indices = &indices } };
    const encoded_value: Batch = .{ .dictionary = .{ .values = &.{value}, .indices = &indices } };
    for ([_]bool{ false, true }) |encoded| for (std.enums.values(Mode)) |mode| {
        Clock.tick = 0;
        var signal: std.atomic.Value(bool) = .init(false);
        var backing = std.testing.FailingAllocator.init(a, .{ .fail_index = if (mode == .backing_oom) 1 else std.math.maxInt(usize) });
        var node: Node = .{ .mode = mode, .signal = &signal, .backing = &backing, .metadata_bytes = metadata };
        var memory: Budget.Partitioned = .{ .foreground = .{ .child = backing.allocator(), .limit = 1 << 20, .accounting = .{ .ptr = &node, .reserve_fn = Node.reserve, .try_reserve_fn = Node.attempt, .release_fn = Node.release } }, .control = .{ .child = a, .limit = 64 } };
        var server: http_server.ApiHttpServer = undefined;
        server.alloc = a;
        server.request_memory = &memory;
        server.embedding_provider_runtime.io = std.testing.io;
        var scheduler = try http_server.RunnableScheduler.Scheduler.init(.{ .slots = 2 });
        defer scheduler.close();
        var execution: http_server.RunnableScheduler.Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100, .retained_bytes = 72, .cancellation = .{ .ptr = &signal, .requested = Node.canceled } }, .query);
        defer execution.finish();
        var identity: ?http_server.AuthenticatedIdentity = null;
        var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .runnable = &execution, .serial_execution = true };
        try adapter.beginReadFence();
        defer adapter.endReadFence();
        const HashCheck = struct {
            adapter: *Adapter,
            node: *Node,
            signal: *std.atomic.Value(bool),
            triggered: bool = false,
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.node.mode == .estimate_canceled or (self.node.mode == .hash_canceled and self.node.backing.allocated_bytes - self.node.backing.freed_bytes == self.node.metadata_bytes + scratch_bytes)) {
                    // Estimation stops before reservation; the later hash
                    // mode stops only when the vector is physically live.
                    std.debug.assert(self.node.backing.allocated_bytes - self.node.backing.freed_bytes == (if (self.node.mode == .estimate_canceled) @as(usize, 0) else self.node.metadata_bytes + scratch_bytes));
                    self.triggered = true;
                    self.signal.store(true, .release);
                }
                try Adapter.checkpoint(self.adapter);
            }
        };
        var hash_check: HashCheck = .{ .adapter = &adapter, .node = &node, .signal = &signal };
        var scope = adapter.coordinatorScope().?;
        scope.check = .{ .context = &hash_check, .check = HashCheck.check };
        const group = try Groups.create(a, &specs, .{ .coordinator_scope = scope });
        defer group.deinit();
        const result = if (encoded) group.addEncodedColumns(&.{encoded_key}, &.{ encoded_value, encoded_value }, count) else group.addColumns(&.{&keys}, &.{ &values, &values }, count);
        if (mode == .canceled or mode == .expired or mode == .hash_canceled or mode == .estimate_canceled) {
            try std.testing.expectError(if (mode == .expired) error.DeadlineExceeded else error.Canceled, result);
            try std.testing.expectEqual(@as(u64, 0), group.rows_seen);
            try std.testing.expectEqual(@as(usize, 0), group.groups.items.len);
        } else {
            try result;
            const reference = try Groups.create(a, &specs, .{});
            defer reference.deinit();
            for (0..count) |_| try reference.add(&.{key}, &.{ value, value });
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const actual = try group.resultAt(arena.allocator(), 0);
            const expected = try reference.resultAt(arena.allocator(), 0);
            try std.testing.expectEqualDeep(expected.keys, actual.keys);
            try std.testing.expectEqualDeep(expected.aggregates, actual.aggregates);
            try std.testing.expectEqual(@as(u64, count), group.rows_seen);
        }
        try std.testing.expectEqual(mode == .hash_canceled or mode == .estimate_canceled, hash_check.triggered);
        try std.testing.expectEqual(@as(usize, if (mode == .estimate_canceled) 0 else 1), node.attempts);
        try std.testing.expectEqual(if (mode == .estimate_canceled) @as(usize, 0) else metadata + @as(usize, if (mode == .denied) 0 else scratch_bytes), node.peak);
        try std.testing.expectEqual(mode != .denied and mode != .estimate_canceled, node.physical_retirement_checked);
        try std.testing.expectEqual(backing.allocated_bytes, backing.freed_bytes);
        try std.testing.expectEqual(@as(usize, 0), node.used);
        try std.testing.expectEqual(@as(usize, 0), memory.foreground.stats().used_bytes);
        try std.testing.expectEqual(@as(usize, 0), memory.control.stats().used_bytes);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 72), scheduler.stats().retained_bytes);
        try std.testing.expectEqual(@as(?u64, 100), execution.entry.?.options.deadline_ns);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    };
}

test "SQL pgwire native read cursor keeps transported JSON encoded through column pages and retires cache owners" {
    const Sources = @import("antfly_local_sources");
    const View = Sources.storage_relational_read_view.View;
    const Payload = @import("retained_read_page.zig");
    const Fixture = struct {
        bytes: []const u8,
        delivered: bool = false,
        fn next(raw: *anyopaque, a: std.mem.Allocator, _: u32) !View.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.delivered) return .{ .arena = .init(a), .rows = &.{}, .after = null };
            self.delivered = true;
            return Payload.decode(a, self.bytes, 1, 3, 1);
        }
        fn close(_: *anyopaque) void {}
    };
    const a = std.testing.allocator;
    var source = std.heap.ArenaAllocator.init(a);
    defer source.deinit();
    const sa = source.allocator();
    const json = try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"x\":[9007199254740993]}", .{ .parse_numbers = false });
    const layout = try catalog.Row.TypedLayout.init(sa, &.{"j"});
    const encoded_page = try Payload.encodeVersion(sa, 1, &.{.{ .id = "key", .version = 1, .schema_version = 3, .value = .null, .typed_cells = .{ .layout = layout, .values = &.{Sources.sql_scalar.Datum.json(json)} } }}, null, .v2);
    var fixture: Fixture = .{ .bytes = encoded_page };
    // Page delivery still crosses the normal checkpoint and admission path.
    // This fixture has no server/catalog work and no retained read fence.
    var adapter: Adapter = .{ .server = undefined, .identity = undefined, .context = .{} };
    const table: catalog.Table = .{ .id = 1, .physical_name = "docs", .schema_version = 3, .columns = &.{.{ .name = "j", .path = "j", .type = .json }} };
    var projection = try Sources.sql_document_row.Projection.init(a, table, &.{"j"});
    defer projection.deinit(a);
    var budget: Sources.sql_memory_budget = .{ .backing = a, .limit = 1 << 20 };
    var cursor: Adapter.ReadCursor = .{ .alloc = budget.allocator(), .adapter = &adapter, .schema_version = 3, .projection = projection, .view = .{ .ptr = &fixture, .vtable = &.{ .next = Fixture.next, .close = Fixture.close } } };
    defer cursor.retireColumns();
    cursor.require_primary_digest = true;
    try std.testing.expectError(error.UnsupportedSqlExecution, Adapter.ReadCursor.nextColumns(&cursor, a, 1));
    try std.testing.expect(!fixture.delivered);
    cursor.require_primary_digest = false;
    const columns = try Adapter.ReadCursor.nextColumns(&cursor, a, 1);
    try std.testing.expectEqualStrings("{\"x\":[9007199254740993]}", (try columns.encodedJson(0, "j")).?);
    try std.testing.expectEqualStrings("key", (try columns.cell(sa, 0, "_id")).value.string);
    const page_row = cursor.column_page.?.rows[0];
    const columnar = page_row.typed_cells.?.columnar.?.blocks[0];
    const range = (try columnar.frozenJson(0, 0, false)).?;
    const frozen = columnar.json_owner.?.cached.get(@intFromPtr(range.bytes.ptr)).?;
    try std.testing.expect(frozen.value == null);
    const end = try Adapter.ReadCursor.nextColumns(&cursor, a, 1);
    try std.testing.expectEqual(@as(usize, 0), end.selection.len);
    cursor.retireColumns();
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

test "public execution SQL estimates use planning metadata without row authority" {
    const a = std.testing.allocator;
    const planning = @import("join_planning.zig");
    const generation = try a.create(planning.Generation);
    generation.* = .{ .alloc = a, .arena = .init(a) };
    defer generation.release();
    try generation.tables.put(generation.arena.allocator(), "physical", .{ .table_id = 7, .name = "physical", .stats = .{ .row_count = 100, .row_count_known = true, .size_bytes = 4096, .size_bytes_known = true } });
    const Fake = struct {
        generation: *planning.Generation,
        captures: usize = 0,
        fail: bool = false,
        fn capture(raw: *anyopaque, budget: @import("table_router.zig").RouteBudget) !?*planning.Generation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try budget.check();
            self.captures += 1;
            if (self.fail) return error.MockMetadataFailure;
            return self.generation.retain();
        }
        fn resolve(_: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            try std.testing.expect(call == .resolve_many);
            return alloc.dupe(u8, "{\"revision\":3,\"tables\":[{\"table_id\":7,\"name\":\"physical\"}]}");
        }
    };
    var fake: Fake = .{ .generation = generation };
    // All row, search, mutation and tracking authorities remain poisoned.
    var server: http_server.ApiHttpServer = undefined;
    server.source = .{ .ptr = &fake, .vtable = &.{ .status = undefined, .system_catalog = Fake.resolve, .acquire_join_planning = Fake.capture } };
    var identity: ?http_server.AuthenticatedIdentity = null;
    var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{} };
    const table: catalog.Table = .{ .id = 7, .physical_name = "physical", .schema_version = 9, .columns = &.{}, .scope = .{ .database = "d", .namespace = "n", .name = "logical", .revision = 3 } };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scans = [_]catalog.StatementScan{.{ .table = table, .request = .{ .fields = &.{}, .limit = 1 } }};
    const estimates = try Adapter.estimateScans(&adapter, arena.allocator(), &scans);
    try std.testing.expectEqual(@as(?u64, 100), estimates[0].estimated_rows);
    try std.testing.expectEqual(@as(?u64, 4096), estimates[0].estimated_bytes);
    try std.testing.expectEqual(@as(usize, 1), fake.captures);
    generation.tables.getPtr("physical").?.stats.row_count_known = false;
    const unknown = try Adapter.estimateScans(&adapter, arena.allocator(), &scans);
    try std.testing.expect(unknown[0].estimated_rows == null);
    fake.fail = true;
    try std.testing.expectError(error.MockMetadataFailure, Adapter.estimateScans(&adapter, a, &scans));
}
