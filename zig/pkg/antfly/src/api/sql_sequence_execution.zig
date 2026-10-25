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

//! Sequence effects use the native catalog allocator and exact receipt
//! observation. Scalar callbacks only describe demands; all I/O is owned here.
const std = @import("std");
const local = @import("antfly_local_sources");
const effects = local.sql_sequence_effects;
const session = local.sql_sequence_session;
const domain = local.system_catalog_domain;
const sequences = @import("../system_catalog/sequence_catalog.zig");
const execution = @import("sql_execution.zig");
const http = @import("http_server.zig");
const A = std.mem.Allocator;
const Live = struct { reference: effects.Reference, group: u64, definition: sequences.sequence.Definition, counter: sequences.sequence.State };

fn snapshot(root: *execution.Adapter, a: A, request: sequences.SnapshotRequest) !sequences.Snapshot {
    try root.context.ensureActive();
    if (!root.server.source.vtable.supports_sql_sequences) return error.SequenceProviderUnavailable;
    var context = root.context;
    context.setting_admin = true;
    const bytes = try root.server.source.systemCatalog(a, context, .{ .sql_sequence_snapshot = request });
    defer a.free(bytes);
    const result = try std.json.parseFromSliceLeaky(sequences.Snapshot, a, bytes, .{ .allocate = .alloc_always });
    try result.validate(request);
    return result;
}
fn liveFrom(cut: sequences.Snapshot, entry: sequences.Snapshot.Entry) !Live {
    if (entry.value == .other) return error.SqlWrongObjectType;
    if (entry.value != .sequence) return error.SequenceNotFound;
    if (cut.metadata_group_id == 0) return error.InvalidSqlBackendResponse;
    return .{
        .reference = .{ .identity = .{ .incarnation = cut.incarnation, .id = entry.value.sequence.id }, .namespace_id = entry.namespace_id.?, .target = entry.target },
        .group = cut.metadata_group_id,
        .definition = try entry.value.sequence.definition(),
        .counter = entry.counter orelse return error.InvalidSqlBackendResponse,
    };
}
fn byIdentity(root: *execution.Adapter, a: A, identity: session.Identity) !Live {
    const request: sequences.SnapshotRequest = .{ .targets = &.{.{ .database = root.database, .namespace = root.namespace, .table = "_sequence_identity" }}, .sequence_ids = &.{identity.id}, .incarnation = identity.incarnation, .include_counters = true };
    const cut = try snapshot(root, a, request);
    return liveFrom(cut, cut.entries[0]);
}
fn capture(root: *execution.Adapter, a: A, name: []const u8, prepared: bool) !effects.Reference {
    const parsed = try local.sql_object_name.parse(a, name);
    const path = root.ddl_search_path;
    const unqualified = parsed.database == null and parsed.namespace == null and path != null;
    const count: usize = if (unqualified) path.?.len else 1;
    var targets: [@import("../pgwire/search_path.zig").Path.max_namespaces]domain.Target = undefined;
    var used: usize = 0;
    for (0..count) |ordinal| {
        const target: domain.Target = .{ .database = parsed.database orelse root.database, .namespace = if (unqualified) path.?.entries[ordinal].slice() else parsed.namespace orelse root.namespace, .table = parsed.table };
        try target.validate();
        if (!try @import("sql_catalog_auth.zig").namespaceUsage(root.identity.*, a, target)) continue;
        targets[used] = target;
        used += 1;
    }
    if (used == 0) return error.SequenceNotFound;
    var reference: ?effects.Reference = null;
    if (prepared) if (root.prepared_bindings) |bindings| {
        for (targets[0..used]) |target| {
            for (bindings) |binding| {
                if (binding.kind != .sequence or !std.mem.eql(u8, binding.database, target.database) or !std.mem.eql(u8, binding.namespace, target.namespace) or !std.mem.eql(u8, binding.name, target.table)) continue;
                reference = (try byIdentity(root, a, .{ .incarnation = binding.sequence_incarnation orelse return error.InvalidSqlBackendResponse, .id = binding.id })).reference;
                break;
            }
            if (reference != null) break;
        }
        if (reference == null) return error.CatalogGenerationChanged;
    };
    if (reference == null) {
        const cut = try snapshot(root, a, .{ .targets = targets[0..used] });
        for (cut.entries) |entry| {
            if (entry.value == .missing_database) return error.DatabaseNotFound;
            if (entry.value == .missing_namespace or entry.value == .absent) continue;
            if (entry.value != .sequence) return error.SqlWrongObjectType;
            reference = .{ .identity = .{ .incarnation = cut.incarnation, .id = entry.value.sequence.id }, .namespace_id = entry.namespace_id.?, .target = entry.target };
            break;
        }
    }
    const result = reference orelse return error.SequenceNotFound;
    if (root.collect_prepared_bindings) |bindings| try bindings.append(a, .{
        .kind = .sequence,
        .sequence_incarnation = result.identity.incarnation,
        .database = result.target.database,
        .namespace = result.target.namespace,
        .name = result.target.table,
        .physical_name = "",
        .id = result.identity.id,
        .schema_version = 0,
        .revision = 0,
    });
    return result;
}
pub fn bind(raw: *anyopaque, a: A, name: []const u8) !effects.Reference {
    return capture(@ptrCast(@alignCast(raw)), a, name, true);
}

pub const Owner = struct {
    root: *execution.Adapter,
    arena: std.heap.ArenaAllocator,
    state: *session.State,
    state_allocator: A,
    temporary: session.State = .{},
    pooled: ?@import("sql_sequence_cache.zig").Lease = null,
    connection_generation: ?u64 = null,
    connection_sequence_epoch: ?u64 = null,
    transaction: ?[16]u8 = null,
    read_only: bool = false,
    flushed: bool = false,
    live: std.AutoHashMapUnmanaged(session.Identity, Live) = .empty,
    names: std.StringHashMapUnmanaged(session.Identity) = .empty,

    pub fn create(root: *execution.Adapter) !*Owner {
        const a = root.server.alloc;
        const self = try a.create(Owner);
        self.* = .{ .root = root, .arena = std.heap.ArenaAllocator.init(a), .state = undefined, .state_allocator = a };
        self.state = &self.temporary;
        errdefer {
            self.arena.deinit();
            a.destroy(self);
        }
        const principal = http.transactionPrincipal(root.identity.*);
        const node = root.server.localSessionNodeId();
        var state: ?@import("transactions.zig").SessionRegistry.SqlState = null;
        defer if (state) |*value| value.deinit(a);
        if (root.session_id) |encoded| {
            const id = try @import("distributed_txn.zig").parseTxnIdHex(encoded);
            if (root.active_transaction == null or !std.meta.eql(root.active_transaction.?, id)) return error.SqlTransactionNotActive;
            self.transaction = id;
        }
        if (root.active_transaction) |id| {
            state = (try root.server.txn_sessions.getSqlState(a, id)) orelse return error.SqlTransactionNotActive;
            if (state.?.owner_node_id != node) return error.SessionLeaseLost;
            if (!std.meta.eql(state.?.connection_id, root.connection_id)) return error.SqlConnectionNotFound;
            self.read_only = state.?.metadata.mode == .read_only;
        }
        if (root.sequence_state) |borrowed| {
            self.state = borrowed.state;
            self.state_allocator = borrowed.allocator;
        } else if (root.connection_id) |id| {
            const store = root.server.txn_sessions.durable orelse return error.SequenceProviderUnavailable;
            var connection = try @import("sql_connections.zig").load(store, a, id, principal orelse "", node, @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
            defer connection.deinit();
            if (!std.meta.eql(connection.value.active_txn, root.active_transaction)) return error.SqlConnectionBusy;
            self.transaction = connection.value.active_txn;
            self.connection_generation = connection.value.generation;
            self.connection_sequence_epoch = connection.value.sequence_epoch;
            if (root.connection_generation) |expected| if (expected != connection.value.generation) return error.SqlConnectionNotFound;
            self.pooled = try root.server.sql_sequence_sessions.borrow(a, root.server.sharedApiIo() orelse return error.SequenceProviderUnavailable, .{ .kind = .connection, .id = id }, principal orelse "", connection.value.sequence_epoch, node, connection.value.sequence_state);
            self.state = self.pooled.?.state();
        } else if (self.transaction) |id| {
            self.pooled = try root.server.sql_sequence_sessions.borrow(a, root.server.sharedApiIo() orelse return error.SequenceProviderUnavailable, .{ .kind = .transaction, .id = std.fmt.bytesToHex(id, .lower) }, principal orelse "", state.?.sequence_epoch, node, state.?.sequence_state);
            self.state = self.pooled.?.state();
        }
        return self;
    }
    pub fn deinit(self: *Owner) void {
        const a = self.root.server.alloc;
        if (self.pooled) |lease| lease.release(self.root.server.sharedApiIo().?);
        self.temporary.deinit(a);
        self.arena.deinit();
        a.destroy(self);
    }
    fn current(self: *Owner, identity: session.Identity) !*Live {
        if (self.live.getPtr(identity)) |entry| return entry;
        if (self.live.count() == session.max_sequences) return error.SqlProgramLimitExceeded;
        const a = self.arena.allocator();
        const value = try byIdentity(self.root, a, identity);
        const slot = try self.live.getOrPut(a, identity);
        slot.value_ptr.* = value;
        return slot.value_ptr;
    }
    fn authorize(self: *Owner, a: A, value: Live, mutating: bool) !void {
        const resource = try value.reference.target.resourceNameAlloc(a);
        const identity = self.root.identity.*;
        if (self.root.server.cfg.auth_enabled and identity == null) return error.Forbidden;
        if (!try @import("sql_catalog_auth.zig").namespaceUsage(identity, a, value.reference.target)) return error.Forbidden;
        if (!try http.tablePermissionCurrentlyAllowed(identity, resource, if (mutating) .write else .read)) return error.Forbidden;
        if (mutating and self.read_only) return error.SqlReadOnlyTransaction;
    }
    fn allocate(self: *Owner, live: *Live, operation: sequences.journal.Operation) !sequences.journal.Receipt {
        var scratch = std.heap.ArenaAllocator.init(self.root.server.alloc);
        defer scratch.deinit();
        const a = scratch.allocator();
        var context = self.root.context;
        context.setting_admin = true;
        const io = self.root.server.sharedApiIo() orelse return error.SequenceProviderUnavailable;
        for (0..32) |_| {
            try context.ensureActive();
            var nonce: [16]u8 = undefined;
            try io.randomSecure(&nonce);
            const request: sequences.CounterRequest = .{ .target = live.reference.target, .namespace_id = live.reference.namespace_id, .sequence_id = live.reference.identity.id, .incarnation = live.reference.identity.incarnation, .request = .{ .expected = live.counter, .request_id = nonce, .operation = operation } };
            const key: sequences.journal.Key = .{ .group = live.group, .sequence_id = live.reference.identity.id };
            _ = try sequences.journal.prepare(key, live.definition, live.counter, request.request, null);
            const bytes = self.root.server.source.systemCatalog(a, context, .{ .sql_sequence_counter_mutate = request }) catch null;
            var receipt: ?sequences.journal.Receipt = null;
            if (bytes) |encoded| {
                const outcome = std.json.parseFromSliceLeaky(sequences.CounterOutcome, a, encoded, .{}) catch return error.SqlSequenceOutcomeUnknown;
                receipt = outcome.result() catch |err| switch (err) {
                    error.SequenceCounterChanged, error.SequenceCatalogChanged => {
                        var fresh = try byIdentity(self.root, a, live.reference.identity);
                        try self.authorize(a, fresh, true);
                        const current_target = live.reference.target;
                        if (std.mem.eql(u8, fresh.reference.target.database, current_target.database) and std.mem.eql(u8, fresh.reference.target.namespace, current_target.namespace) and std.mem.eql(u8, fresh.reference.target.table, current_target.table)) fresh.reference.target = current_target else {
                            const retained = self.arena.allocator();
                            fresh.reference.target.database = try retained.dupe(u8, fresh.reference.target.database);
                            fresh.reference.target.namespace = try retained.dupe(u8, fresh.reference.target.namespace);
                            fresh.reference.target.table = try retained.dupe(u8, fresh.reference.target.table);
                        }
                        live.* = fresh;
                        _ = scratch.reset(.retain_capacity);
                        continue;
                    },
                    else => return err,
                };
            } else {
                // Observe the exact original request. An absent, evicted or
                // no-longer-current result is unknown, never a fresh retry.
                const recovered = self.root.server.source.systemCatalog(a, context, .{ .sql_sequence_counter_outcome = request }) catch return error.SqlSequenceOutcomeUnknown;
                const admission = std.json.parseFromSliceLeaky(sequences.CounterAdmission, a, recovered, .{}) catch return error.SqlSequenceOutcomeUnknown;
                receipt = if (admission == .applied) admission.applied else return error.SqlSequenceOutcomeUnknown;
            }
            _ = sequences.journal.prepare(key, live.definition, receipt.?.state, request.request, receipt) catch return error.SqlSequenceOutcomeUnknown;
            live.counter = receipt.?.state;
            return receipt.?;
        }
        return error.SqlSequenceContention;
    }
    fn dynamic(self: *Owner, name: []const u8) !session.Identity {
        if (self.names.get(name)) |identity| return identity;
        if (self.names.count() == session.max_sequences) return error.SqlProgramLimitExceeded;
        var scratch = std.heap.ArenaAllocator.init(self.root.server.alloc);
        defer scratch.deinit();
        const reference = try capture(self.root, scratch.allocator(), name, false);
        const retained = self.arena.allocator();
        try self.names.put(retained, try retained.dupe(u8, name), reference.identity);
        return reference.identity;
    }
    pub fn invoke(self: *Owner, a: A, function: effects.Function, reference: ?effects.Reference, args: []const local.sql_scalar.Datum) !i64 {
        self.flushed = false;
        try self.root.context.ensureActive();
        const identity = if (function == .lastval) (try self.state.lastValue()).identity else if (reference) |bound| bound.identity else try self.dynamic(args[0].value.string);
        const live = self.current(identity) catch |err| {
            if (function == .lastval and err == error.SequenceNotFound) return error.SqlSequenceValueNotDefined;
            return err;
        };
        try self.authorize(a, live.*, function == .nextval or function == .setval);
        if (function == .currval or function == .lastval) return self.state.current(identity);
        const slot = try self.state.prepare(self.state_allocator, identity);
        switch (function) {
            .nextval => {
                if (try slot.take(live.definition.generation)) |value| {
                    self.state.publishNext(identity, value);
                    return value;
                }
                const receipt = try self.allocate(live, .reserve);
                try slot.install(receipt.lease orelse return error.SqlSequenceOutcomeUnknown);
                const value = (try slot.take(live.definition.generation)) orelse return error.SqlSequenceOutcomeUnknown;
                self.state.publishNext(identity, value);
                return value;
            },
            .setval => {
                const value = args[1].value.integer;
                const called = if (args.len == 2) true else args[2].value.bool;
                _ = try self.allocate(live, .{ .set = .{ .value = value, .called = called } });
                self.state.publishSet(identity, value, called);
                return value;
            },
            else => unreachable,
        }
    }
    pub fn flush(self: *Owner) !void {
        if (self.flushed) return;
        const root = self.root;
        const a = root.server.alloc;
        if (!self.state.dirty) {
            if (root.connection_id) |id| {
                const store = root.server.txn_sessions.durable orelse return error.SequenceProviderUnavailable;
                var observed = try @import("sql_connections.zig").load(store, a, id, http.transactionPrincipal(root.identity.*) orelse "", root.server.localSessionNodeId(), @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
                defer observed.deinit();
                if (observed.value.generation != self.connection_generation.? or observed.value.sequence_epoch != self.connection_sequence_epoch.? or !std.meta.eql(observed.value.active_txn, self.transaction)) return error.SqlConnectionChanged;
            }
            self.flushed = true;
            return;
        }
        if (root.sequence_state != null or (root.connection_id == null and self.transaction == null)) {
            self.state.dirty = false;
            self.flushed = true;
            return;
        }
        var snapshot_value = try self.state.snapshot(a);
        defer snapshot_value.deinit(a);
        if (root.connection_id) |id| {
            const store = root.server.txn_sessions.durable orelse return error.SequenceProviderUnavailable;
            _ = try @import("sql_connections.zig").mutate(store, .{ .sequence_state = .{ .id = id, .generation = self.connection_generation.?, .sequence_epoch = self.connection_sequence_epoch.?, .active_txn = self.transaction, .value = snapshot_value } }, http.transactionPrincipal(root.identity.*) orelse "", root.server.localSessionNodeId(), @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
        } else if (self.transaction) |id| {
            try root.server.txn_sessions.setSqlSequenceState(a, id, http.transactionPrincipal(root.identity.*), root.server.localSessionNodeId(), snapshot_value, false);
        }
        self.state.dirty = false;
        self.flushed = true;
    }
};

pub fn invoke(raw: *anyopaque, a: A, function: effects.Function, reference: ?effects.Reference, args: []const local.sql_scalar.Datum) !i64 {
    const root: *execution.Adapter = @ptrCast(@alignCast(raw));
    if (root.sequence_owner == null) root.sequence_owner = try Owner.create(root);
    return root.sequence_owner.?.invoke(a, function, reference, args);
}
pub fn flush(raw: *anyopaque) !void {
    const root: *execution.Adapter = @ptrCast(@alignCast(raw));
    if (root.sequence_owner) |owner| owner.flush() catch return error.SqlSequenceOutcomeUnknown;
}

test "SQL sequence allocator observes the exact lost acknowledgement without replay or forged leases" {
    const Probe = struct {
        mode: enum { applied, ready, unknown, forged },
        definition: sequences.sequence.Definition,
        counter: sequences.sequence.State,
        receipt: ?sequences.journal.Receipt = null,
        admissions: usize = 0,
        observations: usize = 0,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return error.UnexpectedCall;
        }
        fn call(raw: *anyopaque, a: A, _: local.api_operation.RequestContext, input: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return switch (input) {
                .sql_sequence_counter_mutate => |request| blk: {
                    self.admissions += 1;
                    const decision = try sequences.journal.prepare(.{ .group = 7, .sequence_id = 9 }, self.definition, self.counter, request.request, null);
                    self.receipt = decision.apply;
                    self.counter = decision.apply.state;
                    break :blk error.InjectedLostAcknowledgement;
                },
                .sql_sequence_counter_outcome => |request| blk: {
                    self.observations += 1;
                    try std.testing.expectEqualDeep(self.receipt.?, (try sequences.journal.prepare(.{ .group = 7, .sequence_id = 9 }, self.definition, self.counter, request.request, self.receipt)).replay);
                    var outcome: sequences.CounterAdmission = switch (self.mode) {
                        .applied, .forged => .{ .applied = self.receipt.? },
                        .ready => .ready,
                        .unknown => .unknown,
                    };
                    if (self.mode == .forged) outcome.applied.lease.?.first -= 1;
                    break :blk std.json.Stringify.valueAlloc(a, outcome, .{});
                },
                else => error.UnexpectedCall,
            };
        }
    };
    const a = std.testing.allocator;
    inline for (.{ .applied, .ready, .unknown, .forged }) |mode| {
        const definition = try sequences.sequence.Definition.prepare(null, .{ .cache = 4 });
        var probe: Probe = .{ .mode = mode, .definition = definition, .counter = try sequences.sequence.State.init(definition) };
        var api: http.ApiHttpServer = undefined;
        api.alloc = a;
        api.cfg = .{ .imported_runtime_io = .{ .api = std.testing.io } };
        api.source = .{ .ptr = &probe, .vtable = &.{ .status = Probe.status, .system_catalog = Probe.call, .supports_sql_sequences = true } };
        var identity: ?http.AuthenticatedIdentity = null;
        var root: execution.Adapter = .{ .server = &api, .identity = &identity, .context = .{} };
        var state: session.State = .{};
        defer state.deinit(a);
        var owner: Owner = .{ .root = &root, .arena = std.heap.ArenaAllocator.init(a), .state = &state, .state_allocator = a };
        defer owner.arena.deinit();
        var live: Live = .{ .reference = .{ .identity = .{ .incarnation = @splat(1), .id = 9 }, .namespace_id = 2, .target = .{ .table = "numbers" } }, .group = 7, .definition = definition, .counter = probe.counter };
        if (mode == .applied) {
            const receipt = try owner.allocate(&live, .reserve);
            try std.testing.expectEqual(@as(i64, 1), try receipt.lease.?.value(0));
        } else try std.testing.expectError(error.SqlSequenceOutcomeUnknown, owner.allocate(&live, .reserve));
        try std.testing.expectEqual(@as(usize, 1), probe.admissions);
        try std.testing.expectEqual(@as(usize, 1), probe.observations);
        try std.testing.expectEqual(@as(i64, 4), probe.counter.last_value);
    }
}
