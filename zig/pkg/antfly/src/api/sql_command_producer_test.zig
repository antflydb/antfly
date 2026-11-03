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

const std = @import("std");
const local = @import("antfly_local_sources");
const producer = @import("sql_command_producer.zig");
const connections = @import("sql_connections.zig");
const transactions = @import("transactions.zig");
const authority = @import("../system_catalog/lock_authority.zig");
const storage = @import("../metadata/storage/raft_apply_store.zig");
const protocol = @import("../metadata/topology_protocol.zig");
const Call = @import("../system_catalog/server_call.zig").Call;
const A = std.mem.Allocator;
const a = std.testing.allocator;
const group = local.common_group_ids.main_metadata_group_id;
const connection: [32]u8 = @splat('a');
const instance = id(1);
const actor: authority.prepared.Actor = .{ .session_user = .bootstrap, .credential = "" };
fn id(value: u64) [16]u8 {
    var bytes: [16]u8 = @splat(0);
    std.mem.writeInt(u64, bytes[8..16], value, .big);
    return bytes;
}
const Native = struct {
    metadata: *storage.RaftApplyStore,
    replicated: bool = false,
    standalone: bool = false,
    index: u64 = 100,
    serial: u64 = 1000,
    expiry_ms: u64 = 1000,
    database: []const u8 = "main",
    before: bool = false,
    after: bool = false,
    calls: usize = 0,
    outcome_reads: usize = 0,
    scope_reads: usize = 0,
    failure: ?enum { admit, prepare, commit, handoff, ack } = null,
    fail_before: bool = false,
    block_cancel: bool = false,
    previous: ?[32]u8 = null,
    last: ?[32]u8 = null,
    fn nonce(self: *@This()) [16]u8 {
        self.serial += 1;
        return id(self.serial);
    }
    fn port(self: *@This()) producer.Port {
        return .{ .ptr = self, .call = call };
    }
    fn call(raw: *anyopaque, alloc: A, input: Call) anyerror![]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return switch (input) {
            .read => |request| self.metadata.systemCatalogRead(alloc, group, request),
            .sql_prepared_read => |request| self.metadata.sqlPreparedRead(alloc, group, request, .{ .setting_admin = true }),
            .sql_prepared_outcome => |request| self.metadata.sqlPreparedOutcome(alloc, group, request),
            .sql_prepared_mutate => |request| blk: {
                if (request.operation == .cancel_admission and self.block_cancel) return error.MetadataMutationOutcomeUnknown;
                const point: @TypeOf(self.failure) = switch (request.operation) {
                    .admit => .admit,
                    .acknowledge_receipt => .ack,
                    .advance => |change| switch (change.action) {
                        .prepare => .prepare,
                        .commit => .commit,
                        .handoff => .handoff,
                        else => null,
                    },
                    else => null,
                };
                const lose = point != null and point == self.failure;
                if (lose) self.failure = null;
                if (lose and self.fail_before) return error.MetadataMutationOutcomeUnknown;
                const body = try std.json.Stringify.valueAlloc(alloc, request, .{});
                defer alloc.free(body);
                const command: storage.TransitionCommand = .{ .apply_sql_prepared_transaction = body };
                if (self.replicated) {
                    const wire = try storage.encodeTransitionCommand(alloc, command);
                    defer alloc.free(wire);
                    self.index += 1;
                    const entries = try @import("../raft/state_machine/mod.zig").encodeCommittedEntries(alloc, &.{.{ .term = 1, .index = self.index, .entry_type = .normal, .data = wire }});
                    defer alloc.free(entries);
                    try self.metadata.snapshotBuilder().applyBatch(.{ .group_id = group, .commit_index = self.index, .entries_bytes = entries });
                } else try self.metadata.applyStandaloneCommand(group, command);
                if (lose) return error.MetadataMutationOutcomeUnknown;
                break :blk try self.metadata.sqlPreparedOutcome(alloc, group, request);
            },
            .sql_lock_read => |query| blk: {
                self.scope_reads += 1;
                break :blk self.metadata.sqlLockRead(alloc, group, query, .{ .setting_admin = true });
            },
            .sql_lock_outcome => |request| blk: {
                self.outcome_reads += 1;
                break :blk self.metadata.sqlLockOutcome(alloc, group, request);
            },
            .sql_lock_mutate => |request| blk: {
                self.calls += 1;
                self.previous = self.last;
                self.last = try authority.commandDigest(alloc, request);
                if (self.before) {
                    self.before = false;
                    return error.MetadataMutationOutcomeUnknown;
                }
                const body = try std.json.Stringify.valueAlloc(alloc, request, .{});
                defer alloc.free(body);
                const command: storage.TransitionCommand = .{ .apply_sql_lock_scope = body };
                if (self.replicated) {
                    const wire = try storage.encodeTransitionCommand(alloc, command);
                    defer alloc.free(wire);
                    self.index += 1;
                    const entries = try @import("../raft/state_machine/mod.zig").encodeCommittedEntries(alloc, &.{.{ .term = 1, .index = self.index, .entry_type = .normal, .data = wire }});
                    defer alloc.free(entries);
                    try self.metadata.snapshotBuilder().applyBatch(.{ .group_id = group, .commit_index = self.index, .entries_bytes = entries });
                } else try self.metadata.applyStandaloneCommand(group, command);
                if (self.after) {
                    self.after = false;
                    return error.MetadataMutationOutcomeUnknown;
                }
                break :blk try self.metadata.sqlLockOutcome(alloc, group, request);
            },
            else => error.UnexpectedCommandProducerCall,
        };
    }
    fn head(self: *@This()) !u64 {
        const bytes = try self.metadata.sqlLockRead(a, group, .{ .incarnation = @splat(0x11), .standalone = self.standalone, .actor = actor, .database = 1, .backend = .{ .incarnation = @splat(0x11), .id = id(2), .fence = 1 } }, .{ .setting_admin = true });
        defer a.free(bytes);
        var parsed = try std.json.parseFromSlice(authority.Snapshot, a, bytes, .{});
        defer parsed.deinit();
        return parsed.value.head;
    }
    fn step(self: *@This(), source: *transactions.DurableSessionStore, current_instance: [16]u8, now_ms: u64) !void {
        var owned = (try producer.load(source, a, connection)) orelse return error.MissingCommandProducer;
        defer owned.deinit();
        try producer.advance(source, a, "alice", owned.value, current_instance, now_ms, self.nonce(), self.port());
    }
    fn until(self: *@This(), source: *transactions.DurableSessionStore, want: producer.Phase, now_ms: u64) !void {
        for (0..128) |_| {
            if (try phase(source) == want) return;
            try self.step(source, instance, now_ms);
        }
        return error.CommandProducerDidNotConverge;
    }
    fn submit(self: *@This(), value: producer.Record, operation: @FieldType(authority.Request, "operation")) !void {
        const bytes = try call(self, a, .{ .sql_lock_mutate = .{ .admission = .{ .nonce = self.nonce(), .head = try self.head() }, .incarnation = value.registration.lease.backend.incarnation, .standalone = value.registration.standalone, .database = 1, .actor = actor, .backend = value.registration.lease.backend, .operation = operation } });
        defer a.free(bytes);
        var outcome = try std.json.parseFromSlice(?authority.outcomes.Outcome, a, bytes, .{});
        defer outcome.deinit();
        _ = try outcome.value.?.result();
    }
};
fn phase(source: *transactions.DurableSessionStore) !producer.Phase {
    var owned = (try producer.load(source, a, connection)) orelse return error.MissingCommandProducer;
    defer owned.deinit();
    return owned.value.phase;
}
fn setup(metadata: *storage.RaftApplyStore, source: *transactions.DurableSessionStore, standalone: bool, native: *Native) !void {
    native.standalone = standalone;
    try metadata.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = @splat('1') });
    if (standalone) try metadata.replaceStandaloneCatalog(group, try metadata.standaloneRevision(), &.{}, &.{}, "{\"epoch\":1,\"tables\":[],\"ranges\":[]}");
    const activation = try std.json.Stringify.valueAlloc(a, protocol.Activation{ .version = protocol.current_version, .incarnation = @splat('1'), .member_count = 1, .membership_fingerprint = @splat(7) }, .{});
    defer a.free(activation);
    try metadata.applyStandaloneCommand(group, .{ .activate_topology_protocol = activation });
    const node: u64 = if (standalone) 0 else 9;
    const created_at_ms = if (native.expiry_ms == 1000) 1 else native.expiry_ms - 60000;
    _ = try connections.mutate(source, .{ .create = .{ .id = connection, .principal = "alice", .owner_node_id = node, .expires_at_ms = native.expiry_ms, .database = native.database, .namespace = "public" } }, "alice", node, created_at_ms);
    const registration: authority.scopes.Registration = .{ .lease = .{ .backend = .{ .incarnation = @splat(0x11), .id = id(2), .fence = 1 }, .database = 1, .node = node, .principal = try authority.principalDigest(a, .bootstrap), .expires_at_ms = native.expiry_ms }, .connection = connection, .nonce = id(3), .standalone = standalone };
    try producer.create(source, "alice", registration, actor, instance, try native.head(), native.nonce(), created_at_ms);
}
fn begin(native: *Native, source: *transactions.DurableSessionStore, serial: u64) !producer.Ticket {
    var owned = (try producer.load(source, a, connection)).?;
    defer owned.deinit();
    const ticket = try producer.begin(source, "alice", owned.value, instance, .{ .sequence = owned.value.next_command, .nonce = id(serial), .executor = id(serial + 1) }, @splat(8), try native.head(), native.nonce(), 2);
    try native.until(source, .ready, 2);
    return ticket;
}
fn execute(source: *transactions.DurableSessionStore, ticket: producer.Ticket) !authority.scopes.CommandFence {
    var owned = (try producer.load(source, a, connection)).?;
    defer owned.deinit();
    return producer.take(source, "alice", owned.value, instance, ticket, 2);
}

// Real LSM/WAL and the actual metadata receiver; four combinations retain both
// catalog realms and local/Raft apply. Each source snapshot is separately owned.
test "SQL command producer native cold recovery never replays SQL and retires bound scopes incrementally" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |standalone| for ([_]bool{ false, true }) |replicated| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/producer-{any}-{any}", .{ tmp.sub_path, standalone, replicated });
        defer a.free(path);
        var ticket: producer.Ticket = undefined;
        var executed: usize = 0;
        for (0..3) |pass| {
            var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
            defer metadata.deinit();
            var source = transactions.DurableSessionStore.init(a, &metadata.store);
            var native: Native = .{ .metadata = &metadata, .replicated = replicated, .standalone = standalone, .index = 10000 * (pass + 1), .serial = 10000 * (pass + 1) };
            if (pass == 0) {
                try setup(&metadata, &source, standalone, &native);
                native.before = true;
                try std.testing.expectError(error.MetadataMutationOutcomeUnknown, native.step(&source, instance, 2));
                try native.step(&source, instance, 2);
                try std.testing.expectEqualDeep(native.previous.?, native.last.?);
                try native.until(&source, .idle, 2);
                ticket = try begin(&native, &source, 40);
                const fence = try execute(&source, ticket);
                executed += 1;
                {
                    var owned = (try producer.load(&source, a, connection)).?;
                    defer owned.deinit();
                    try std.testing.expectError(error.SqlConnectionBusy, producer.take(&source, "alice", owned.value, instance, ticket, 2));
                    try std.testing.expectError(error.SqlConnectionBusy, connections.mutate(&source, .{ .discard_sequences = connection }, "alice", owned.value.registration.lease.node, 2));
                    _ = try connections.mutateWithCommand(&source, .{ .discard_sequences = connection }, "alice", owned.value.registration.lease.node, 2, &fence);
                    var forged = fence;
                    forged.command.executor = id(99);
                    try std.testing.expectError(error.SqlConnectionBusy, connections.mutateWithCommand(&source, .{ .discard_sequences = connection }, "alice", owned.value.registration.lease.node, 2, &forged));
                    try producer.finish(&source, "alice", owned.value, instance, ticket, try native.head(), native.nonce());
                }
                try native.until(&source, .idle, 2);
                ticket = try begin(&native, &source, 50);
                _ = try execute(&source, ticket);
                executed += 1;
                var owned = (try producer.load(&source, a, connection)).?;
                defer owned.deinit();
                try native.submit(owned.value, .{ .bind = .{ .command = ticket.command, .transaction = id(90) } });
                for (0..3) |resource| try native.submit(owned.value, .{ .effect = .{ .command = ticket.command, .effect = .{ .backend = owned.value.registration.lease.backend, .sequence = resource + 1, .nonce = id(200 + resource), .action = .{ .acquire = .{ .resource = try authority.locks.advisoryResource(@splat(0x11), 1, .{ .single = @intCast(resource) }), .mode = .{ .advisory = .exclusive }, .scope = .session } } } } });
                try producer.requestRecovery(&source, "alice", owned.value);
                try native.step(&source, instance, 2); // persist fence before delivery
                try std.testing.expectEqual(.fencing, try phase(&source));
            } else if (pass == 1) {
                var owned = (try producer.load(&source, a, connection)).?;
                defer owned.deinit();
                try std.testing.expectError(error.SqlConnectionBusy, producer.take(&source, "alice", owned.value, id(999), ticket, 2));
                native.after = true;
                try std.testing.expectError(error.MetadataMutationOutcomeUnknown, native.step(&source, id(999), 2));
                try native.step(&source, id(999), 2); // adopt native fence; stage release
                try native.step(&source, id(999), 2); // release unsealed txn
                try native.step(&source, id(999), 2); // stage first close quantum
                try native.step(&source, id(999), 2); // close at most one resource
                try std.testing.expectEqual(.closing, try phase(&source));
            } else {
                try native.until(&source, .retired, 2);
                var owned = (try producer.load(&source, a, connection)).?;
                defer owned.deinit();
                try std.testing.expectError(error.SqlConnectionBusy, producer.take(&source, "alice", owned.value, instance, ticket, 2));
                const page = try producer.scanWork(&source, a, null, 8);
                defer a.free(page.ids);
                try std.testing.expectEqual(@as(usize, 0), page.ids.len);
                var read = try metadata.store.beginReadTxn();
                defer read.abort();
                try std.testing.expect(try producer.retirementReadyTxn(a, &read, connection));
                const scope = (try authority.scopes.read(a, &read, .{ .home = .metadata, .group = group, .incarnation = @splat(0x11) }, owned.value.registration.lease.backend)).?;
                try std.testing.expectEqual(.retired, scope.state);
                try std.testing.expect(scope.transaction == null);
            }
        }
        try std.testing.expectEqual(@as(usize, 2), executed);
    };
}

test "SQL command producer native shared facades preserve execution ownership lease renewal and admission recovery" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/producer-identity", .{tmp.sub_path});
    defer a.free(path);
    var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
    defer metadata.deinit();
    var source = transactions.DurableSessionStore.init(a, &metadata.store);
    var peer = transactions.DurableSessionStore.init(a, &metadata.store);
    var native: Native = .{ .metadata = &metadata };
    try setup(&metadata, &source, false, &native);
    try native.until(&source, .idle, 2);
    var owned = (try producer.load(&source, a, connection)).?;
    defer owned.deinit();
    const ticket = try producer.begin(&source, "alice", owned.value, instance, .{ .sequence = 1, .nonce = id(10), .executor = id(11) }, @splat(12), try native.head(), native.nonce(), 2);
    // Evict the original admission before its native BEGIN has arrived. Each
    // rejected renewal advances the same existing metadata outcome journal.
    for (0..authority.outcomes.retained) |_| {
        const bytes = try Native.call(&native, a, .{ .sql_lock_mutate = .{ .admission = .{ .nonce = native.nonce(), .head = try native.head() }, .incarnation = @splat(0x11), .standalone = false, .database = 1, .actor = actor, .backend = owned.value.registration.lease.backend, .operation = .{ .renew = .{ .expected_expiry = 999, .expires_at_ms = 2000, .observed_at_ms = 1 } } } });
        a.free(bytes);
    }
    try native.until(&source, .ready, 2);
    {
        var ready = (try producer.load(&source, a, connection)).?;
        defer ready.deinit();
        try std.testing.expectError(error.SqlConnectionBusy, producer.take(&peer, "alice", ready.value, id(200), ticket, 2));
        var wrong = ticket;
        wrong.body = @splat(13);
        try std.testing.expectError(error.SqlConnectionBusy, producer.take(&source, "alice", ready.value, instance, wrong, 2));
        source.fail_writes_for_test = true;
        try std.testing.expectError(error.InjectedSessionStoreFailure, producer.take(&source, "alice", ready.value, instance, ticket, 2));
        source.fail_writes_for_test = false;
        try std.testing.expectEqual(.ready, try phase(&source));
        _ = try producer.take(&source, "alice", ready.value, instance, ticket, 2);
        try std.testing.expectError(error.SqlConnectionBusy, producer.take(&peer, "alice", ready.value, instance, ticket, 2));
    }
    {
        var executing = (try producer.load(&source, a, connection)).?;
        defer executing.deinit();
        try native.step(&peer, id(200), 999);
        try std.testing.expectEqual(.executing, try phase(&source));
        try producer.renew(&source, "alice", executing.value, instance, 3000, try native.head(), native.nonce(), 999);
        native.after = true;
        try std.testing.expectError(error.MetadataMutationOutcomeUnknown, native.step(&source, instance, 999));
        try native.step(&source, instance, 1001); // renew was durable before old expiry
        var renewed = (try producer.load(&source, a, connection)).?;
        defer renewed.deinit();
        try std.testing.expect(!renewed.value.recovery);
        try std.testing.expectEqual(@as(u64, 3000), renewed.value.registration.lease.expires_at_ms);
        try std.testing.expectEqual(.executing, renewed.value.phase);
        const fence: authority.scopes.CommandFence = .{ .backend = renewed.value.registration.lease.backend, .command = ticket.command, .database = 1, .principal = renewed.value.registration.lease.principal, .standalone = false };
        try std.testing.expectError(error.SqlConnectionNotFound, connections.load(&source, a, connection, "alice", 9, 1001));
        var live = try connections.loadWithCommand(&source, a, connection, "alice", 9, 1001, &fence);
        defer live.deinit();
        _ = try connections.mutateWithCommand(&source, .{ .discard_sequences = connection }, "alice", 9, 1001, &fence);
        try std.testing.expectError(error.SqlConnectionBusy, connections.loadWithCommand(&source, a, connection, "alice", 9, 3001, &fence));
        try std.testing.expectError(error.SqlConnectionBusy, connections.mutateWithCommand(&source, .{ .discard_sequences = connection }, "alice", 9, 3001, &fence));
        try std.testing.expectError(error.SqlConnectionBusy, connections.mutate(&peer, .{ .close = connection }, "alice", 9, 2));
    }
    try native.until(&peer, .retired, 3001);
}

test "SQL command producer native connection GC retains uncertain owners and native fences prevent identity resurrection" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/producer-gc", .{tmp.sub_path});
    defer a.free(path);
    var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
    defer metadata.deinit();
    var source = transactions.DurableSessionStore.init(a, &metadata.store);
    var native: Native = .{ .metadata = &metadata };
    try setup(&metadata, &source, true, &native);
    _ = try connections.mutate(&source, .{ .create = .{ .id = @splat('b'), .principal = "alice", .owner_node_id = 0, .expires_at_ms = 3000, .database = "main", .namespace = "public" } }, "alice", 0, 2000);
    {
        var retained = try connections.load(&source, a, connection, "alice", 0, 0);
        defer retained.deinit();
        const work = try producer.scanWork(&source, a, null, 1);
        defer a.free(work.ids);
        try std.testing.expectEqual(@as(usize, 1), work.ids.len);
        try std.testing.expectEqualDeep(connection, work.ids[0]);
        const empty = try producer.scanWork(&source, a, work.next_after, 1);
        defer a.free(empty.ids);
        try std.testing.expectEqual(@as(usize, 0), empty.ids.len);
    }
    try native.until(&source, .retired, 2000);
    _ = try connections.mutate(&source, .{ .create = .{ .id = @splat('c'), .principal = "alice", .owner_node_id = 0, .expires_at_ms = 3000, .database = "main", .namespace = "public" } }, "alice", 0, 2001);
    try std.testing.expectError(error.SqlConnectionNotFound, connections.load(&source, a, connection, "alice", 0, 0));
    try std.testing.expect((try producer.load(&source, a, connection)) == null);
    // Source state is reclaimed with its parent; the native retired binding,
    // rather than another growing local tombstone family, rejects resurrection.
    _ = try connections.mutate(&source, .{ .create = .{ .id = connection, .principal = "alice", .owner_node_id = 0, .expires_at_ms = 3000, .database = "main", .namespace = "public" } }, "alice", 0, 2001);
    const replacement: authority.scopes.Registration = .{ .lease = .{ .backend = .{ .incarnation = @splat(0x11), .id = id(500), .fence = 1 }, .database = 1, .node = 0, .principal = try authority.principalDigest(a, .bootstrap), .expires_at_ms = 3000 }, .connection = connection, .nonce = id(501), .standalone = true };
    try producer.create(&source, "alice", replacement, actor, instance, try native.head(), native.nonce(), 2001);
    for (0..8) |_| {
        var current = (try producer.load(&source, a, connection)).?;
        defer current.deinit();
        if (current.value.phase == .retired) break;
        try producer.advance(&source, a, "alice", current.value, instance, 2001, native.nonce(), native.port());
    }
    var rejected = (try producer.load(&source, a, connection)).?;
    defer rejected.deinit();
    try std.testing.expectEqual(.retired, rejected.value.phase);
    try std.testing.expectEqual(.CatalogGenerationChanged, rejected.value.failure.?);
}

test "SQL command producer native supervisor uses existing authenticated transport without executing abandoned SQL" {
    const Server = @import("http_server.zig").ApiHttpServer;
    const Forward = struct {
        fn status(_: *anyopaque) anyerror!@import("../metadata/api.zig").MetadataStatus {
            return error.UnexpectedStatusRead;
        }
        fn catalog(raw: *anyopaque, alloc: A, context: local.api_operation.RequestContext, input: Call) anyerror![]u8 {
            try std.testing.expect(context.setting_admin);
            try std.testing.expect(context.sql_completion == null);
            if (context.sql_completion_submission) |submitted| submitted.* = true;
            try context.ensureActive();
            return Native.call(raw, alloc, input);
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/producer-supervisor", .{tmp.sub_path});
    defer a.free(path);
    var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
    defer metadata.deinit();
    var source = transactions.DurableSessionStore.init(a, &metadata.store);
    var native: Native = .{ .metadata = &metadata };
    try setup(&metadata, &source, true, &native);
    try native.until(&source, .idle, 2);
    const ticket = try begin(&native, &source, 100);
    _ = try execute(&source, ticket);
    var runtime = try local.storage_db_selected_root.db.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var server = Server.init(a, .{ .backend_runtime = runtime.ptr(), .session_store = &source, .session_maintenance_local_only = true, .deployment_mode = .standalone }, .{ .ptr = &native, .vtable = &.{ .status = Forward.status, .system_catalog = Forward.catalog } }, null, null);
    defer server.deinit();
    const boot = try server.sql_connection_admission.executionInstance(server.embedding_provider_runtime.io);
    try std.testing.expectEqualDeep(boot, try server.sql_connection_admission.executionInstance(server.embedding_provider_runtime.io));
    try std.testing.expect(!std.mem.eql(u8, &boot, &instance));
    var sql_submitted = false;
    var control: producer.NativePort = .{ .server = &server, .context = .{ .sql_completion_submission = &sql_submitted } };
    const bytes = try control.port().call(control.port().ptr, a, .{ .sql_lock_mutate = .{ .admission = .{ .nonce = native.nonce(), .head = try native.head() }, .incarnation = @splat(0x11), .standalone = true, .database = 1, .actor = actor, .backend = .{ .incarnation = @splat(0x11), .id = id(2), .fence = 1 }, .operation = .{ .begin = ticket.command } } });
    defer a.free(bytes);
    try std.testing.expect(!sql_submitted);
    for (0..32) |_| {
        if (try phase(&source) == .retired) break;
        // The supervisor interval is a scheduling policy, not part of source
        // correctness. Drive its real callback without wall-clock sleeps.
        server.sql_command_work_due_ns = 0;
        try server.runSessionMaintenanceOnce();
    }
    try std.testing.expectEqual(.retired, try phase(&source));
    try std.testing.expect(native.calls < 32);
}

test "SQL command producer native providers must prove transaction-wide shared writer serialization" {
    var backend = local.storage_mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{ .name = "command-producer-provider" });
    defer runtime.deinit();
    var unsupported = runtime;
    unsupported.write_gate = null;
    unsupported.write_serialization = .unsupported;
    var store = transactions.DurableSessionStore.initRuntime(a, &unsupported);
    const registration: authority.scopes.Registration = .{ .lease = .{ .backend = .{ .incarnation = @splat(0x11), .id = id(2), .fence = 1 }, .database = 1, .node = 0, .principal = try authority.principalDigest(a, .bootstrap), .expires_at_ms = 1000 }, .connection = connection, .nonce = id(3), .standalone = true };
    try std.testing.expectError(error.SqlConnectionUnavailable, producer.create(&store, "alice", registration, actor, instance, 0, id(4), 1));
    try std.testing.expect((try producer.load(&store, a, connection)) == null);
    const work = try producer.scanWork(&store, a, null, 1);
    defer a.free(work.ids);
    try std.testing.expectEqual(@as(usize, 0), work.ids.len);
    // The actual runtime advertises its shared gate. Distinct facades of that
    // same namespace can coordinate the source record instead of pretending a
    // per-facade mutex serializes provider transactions.
    var supported = transactions.DurableSessionStore.initRuntime(a, &runtime);
    try std.testing.expect(supported.supportsSharedMutations());
    _ = try connections.mutate(&supported, .{ .create = .{ .id = connection, .principal = "alice", .owner_node_id = 0, .expires_at_ms = 1000, .database = "main", .namespace = "public" } }, "alice", 0, 1);
    try producer.create(&supported, "alice", registration, actor, instance, 0, id(4), 1);
    try std.testing.expectEqual(.registering, try phase(&supported));
}

test "SQL command producer native idle facade handoff stages one command and cannot adopt ready or executing SQL" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/producer-idle-handoff", .{tmp.sub_path});
    defer a.free(path);
    var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
    defer metadata.deinit();
    var source = transactions.DurableSessionStore.init(a, &metadata.store);
    var peer = transactions.DurableSessionStore.init(a, &metadata.store);
    var native: Native = .{ .metadata = &metadata };
    try setup(&metadata, &source, false, &native);
    try native.until(&source, .idle, 2);
    var idle = (try producer.load(&source, a, connection)).?;
    defer idle.deinit();
    const before = native.calls;
    const ticket = try producer.beginObserved(&peer, a, "alice", idle.value, id(500), .{ .sequence = 1, .nonce = id(600), .executor = id(601) }, @splat(9), native.nonce(), 2, native.port());
    try std.testing.expectEqual(before, native.calls); // one local CAS, no proposal yet
    try std.testing.expectError(error.SqlConnectionChanged, producer.begin(&source, "alice", idle.value, instance, .{ .sequence = 1, .nonce = id(700), .executor = id(701) }, @splat(8), try native.head(), native.nonce(), 2));
    try native.until(&source, .ready, 2);
    try std.testing.expectEqual(before + 1, native.calls);
    var ready = (try producer.load(&source, a, connection)).?;
    defer ready.deinit();
    try std.testing.expect(ready.value.registration.lease.backend.eql(idle.value.registration.lease.backend));
    try std.testing.expectEqualDeep(id(500), ready.value.instance);
    try std.testing.expectError(error.SqlConnectionBusy, producer.beginObserved(&source, a, "alice", ready.value, instance, .{ .sequence = 1, .nonce = id(700), .executor = id(701) }, @splat(8), native.nonce(), 2, native.port()));
    try std.testing.expectError(error.SqlConnectionBusy, producer.take(&source, "alice", ready.value, instance, ticket, 2));
    _ = try producer.take(&peer, "alice", ready.value, id(500), ticket, 2);
    var executing = (try producer.load(&source, a, connection)).?;
    defer executing.deinit();
    try std.testing.expectError(error.SqlConnectionBusy, producer.beginObserved(&source, a, "alice", executing.value, instance, .{ .sequence = 1, .nonce = id(700), .executor = id(701) }, @splat(8), native.nonce(), 2, native.port()));
    try std.testing.expectError(error.SqlConnectionBusy, producer.finish(&source, "alice", executing.value, instance, ticket, try native.head(), native.nonce()));
    try producer.finish(&peer, "alice", executing.value, id(500), ticket, try native.head(), native.nonce());
    try native.until(&source, .idle, 2);
    var completed = (try producer.load(&source, a, connection)).?;
    defer completed.deinit();
    _ = try producer.beginObserved(&source, a, "alice", completed.value, instance, .{ .sequence = 2, .nonce = id(700), .executor = id(701) }, @splat(8), native.nonce(), 2, native.port());
    try native.until(&source, .ready, 2);
    var next = (try producer.load(&source, a, connection)).?;
    defer next.deinit();
    try std.testing.expectEqual(@as(u64, 2), next.value.next_command);
    try std.testing.expectEqualDeep(instance, next.value.instance);
}

test "SQL command producer native orphaned connections retire through native fences without evaluating SQL" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/producer-orphan", .{tmp.sub_path});
    defer a.free(path);
    var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
    defer metadata.deinit();
    var source = transactions.DurableSessionStore.init(a, &metadata.store);
    var native: Native = .{ .metadata = &metadata };
    try setup(&metadata, &source, true, &native);
    try native.until(&source, .idle, 2);
    const ticket = try begin(&native, &source, 20);
    _ = try execute(&source, ticket);
    var executing = (try producer.load(&source, a, connection)).?;
    defer executing.deinit();
    // Simulate missing parent state during recovery. Its absence cannot become
    // an abort decision, but neither may it strand the immutable cleanup outbox.
    {
        var txn = try metadata.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(&try @import("sql_connection_record.zig").key(&connection));
        try txn.commit();
    }
    try producer.requestRecovery(&source, "alice", executing.value);
    for (0..32) |_| {
        var current = (try producer.load(&source, a, connection)) orelse break;
        defer current.deinit();
        try producer.advance(&source, a, "alice", current.value, id(900), 2, native.nonce(), native.port());
    }
    try std.testing.expect((try producer.load(&source, a, connection)) == null);
    const work = try producer.scanWork(&source, a, null, 1);
    defer a.free(work.ids);
    try std.testing.expectEqual(@as(usize, 0), work.ids.len);
    const observed = try Native.call(&native, a, .{ .sql_lock_read = .{ .incarnation = @splat(0x11), .standalone = true, .actor = actor, .database = 1, .backend = executing.value.registration.lease.backend } });
    defer a.free(observed);
    var snapshot = try std.json.parseFromSlice(authority.Snapshot, a, observed, .{});
    defer snapshot.deinit();
    try std.testing.expectEqual(.retired, snapshot.value.scope.?.state);
    try std.testing.expectError(error.SqlConnectionNotFound, producer.take(&source, "alice", executing.value, instance, ticket, 2));
}

test "SQL command producer native closure survives a delayed renewal after its outbox was superseded" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/producer-late-renewal", .{tmp.sub_path});
    defer a.free(path);
    var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
    defer metadata.deinit();
    var source = transactions.DurableSessionStore.init(a, &metadata.store);
    var native: Native = .{ .metadata = &metadata };
    try setup(&metadata, &source, false, &native);
    try native.until(&source, .idle, 2);
    const ticket = try begin(&native, &source, 80);
    _ = try execute(&source, ticket);
    {
        var executing = (try producer.load(&source, a, connection)).?;
        defer executing.deinit();
        try producer.renew(&source, "alice", executing.value, instance, 3000, try native.head(), native.nonce(), 999);
    }
    var pending = (try producer.load(&source, a, connection)).?;
    defer pending.deinit();
    const renewal = pending.value.pending.?;
    try producer.requestRecovery(&source, "alice", pending.value);
    try native.step(&source, instance, 999);
    try std.testing.expectEqual(.fencing, try phase(&source));
    // The old renewal RPC arrives after closure replaced its source outbox.
    const acknowledged = try Native.call(&native, a, .{ .sql_lock_mutate = renewal });
    defer a.free(acknowledged);
    var result = try std.json.parseFromSlice(?authority.outcomes.Outcome, a, acknowledged, .{});
    defer result.deinit();
    _ = try result.value.?.result();
    try native.until(&source, .retired, 999);
    var retired = (try producer.load(&source, a, connection)).?;
    defer retired.deinit();
    try std.testing.expectEqual(@as(u64, 3000), retired.value.registration.lease.expires_at_ms);
    try std.testing.expect(retired.value.recovery);
    try std.testing.expectError(error.SqlConnectionBusy, producer.take(&source, "alice", retired.value, instance, ticket, 999));
}

test "SQL command producer native retired sources preserve independent connection begin and terminal reconciliation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |terminal| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/producer-reconcile-{any}", .{ tmp.sub_path, terminal });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        var registry = transactions.SessionRegistry.init(&source);
        defer registry.deinit(a);
        var native: Native = .{ .metadata = &metadata };
        try setup(&metadata, &source, true, &native);
        try native.until(&source, .idle, 2);
        const ticket = try begin(&native, &source, 60);
        const fence = try execute(&source, ticket);
        const txn_id = id(90);
        _ = try connections.mutateWithCommand(&source, .{ .begin = .{ .id = connection, .expected_revision = 1, .txn_id = txn_id } }, "alice", 0, 2, &fence);
        if (terminal) {
            _ = try connections.mutateWithCommand(&source, .{ .bind = .{ .id = connection, .txn_id = txn_id } }, "alice", 0, 2, &fence);
            var session: transactions.Session = .{ .txn_id = txn_id, .owner_node_id = 0, .principal = try a.dupe(u8, "alice"), .connection_id = connection, .begin_timestamp = 1, .last_touched_timestamp = 1, .sync_level = .propose, .commit_body_digest = @splat(4), .commit_execution_started = true, .terminal_abort = true, .sql = .{ .database = try a.dupe(u8, "main"), .namespace = try a.dupe(u8, "public"), .isolation = .read_committed, .mode = .read_write } };
            defer session.deinit(a);
            try source.save(session, null);
        }
        {
            var executing = (try producer.load(&source, a, connection)).?;
            defer executing.deinit();
            try producer.requestRecovery(&source, "alice", executing.value);
        }
        try native.until(&source, .retired, 2);
        var restored = try connections.loadReconciled(&source, &registry, a, connection, "alice", 0, 2);
        defer restored.deinit();
        try std.testing.expectEqual(.idle, restored.value.state);
        try std.testing.expect(restored.value.active_txn == null);
        _ = try connections.mutate(&source, .{ .close = connection }, "alice", 0, 2);
        try std.testing.expect((try producer.load(&source, a, connection)) == null);
    }
}

// Drive the production SQL coordinator and its exact local seal through the
// actual metadata receiver. No table transport exists: even an empty COMMIT
// must decide and release its scope, and cold maintenance must finish it.
test "SQL command producer native scoped COMMIT survives every metadata delivery boundary and cold adoption" {
    const coordinator = @import("sql_prepared_coordinator.zig");
    const Forward = struct {
        fn status(_: *anyopaque) anyerror!@import("../metadata/api.zig").MetadataStatus {
            return error.UnexpectedStatusRead;
        }
        fn catalog(raw: *anyopaque, alloc: A, _: local.api_operation.RequestContext, input: Call) anyerror![]u8 {
            return Native.call(raw, alloc, input);
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |replicated| for ([_]bool{ false, true }) |before| {
        for ([_]@TypeOf(@as(Native, undefined).failure){ null, .admit, .prepare, .commit, .handoff, .ack }, 0..) |fault, number| {
            const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/scoped-commit-{any}-{any}-{d}", .{ tmp.sub_path, replicated, before, number });
            defer a.free(path);
            var txn_id: [16]u8 = undefined;
            var operation_id: ?[16]u8 = null;
            for (0..3) |phase_index| {
                var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
                defer metadata.deinit();
                var durable = transactions.DurableSessionStore.init(a, &metadata.store);
                var native: Native = .{ .metadata = &metadata, .replicated = replicated, .standalone = true, .index = 100 + phase_index * 1000 };
                var runtime = try local.storage_db_selected_root.db.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
                defer runtime.deinit();
                var server = @import("http_server.zig").ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr(), .session_store = &durable, .session_maintenance_local_only = true, .deployment_mode = .standalone }, .{ .ptr = &native, .vtable = &.{ .status = Forward.status, .system_catalog = Forward.catalog } }, null, null);
                defer server.deinit();
                if (phase_index == 0) {
                    try setup(&metadata, &durable, true, &native);
                    try native.until(&durable, .idle, 2);
                    const ticket = try begin(&native, &durable, 60);
                    const fence = try execute(&durable, ticket);
                    const session = try server.txn_sessions.beginForPrincipalWithSettings(a, .{ .sync_level = .propose, .sql = .{ .database = "default", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 0, "alice", &.{}, connection);
                    txn_id = session.txn_id;
                    native.failure = fault;
                    native.fail_before = before;
                    var user: ?@import("http_server.zig").AuthenticatedIdentity = null;
                    var owner: @import("sql_session.zig").Coordinator = .{ .server = &server, .identity = &user, .context = .{ .sql_command_fence = &fence } };
                    const result = try @import("sql_session.zig").Coordinator.commit(&owner, .{ .principal = "alice", .database = "default", .namespace = "public" }, txn_id);
                    try std.testing.expectEqualDeep(txn_id, result.reconciliation_id);
                    try std.testing.expect(result.outcome == .committed or result.outcome == .aborted or result.outcome == .unknown);
                    if (fault == null) try std.testing.expectEqual(.committed, result.outcome);
                    var durable_result = (try durable.load(txn_id)).?;
                    defer durable_result.deinit(a);
                    var seal = try std.json.parseFromSlice(transactions.ExternalPreparationSeal, a, durable_result.external_preparation.?, .{});
                    defer seal.deinit();
                    operation_id = seal.value.identity.operation_id;
                    try std.testing.expectEqual(.session_commit, seal.value.identity.kind);
                    try std.testing.expectEqual(.write, seal.value.identity.commit_sync_level);
                    try std.testing.expectEqualDeep(fence.command, seal.value.identity.lock_scope.?.command);
                    try std.testing.expectEqual(@as(usize, 0), seal.value.participants.len);
                } else {
                    var bytes = (try durable.load(txn_id)).?;
                    defer bytes.deinit(a);
                    try std.testing.expect(bytes.external_preparation != null);
                    var region = std.heap.ArenaAllocator.init(a);
                    defer region.deinit();
                    var owner: coordinator.Registry = .{ .server = &server, .context = .{}, .database_id = 1 };
                    var cut = try owner.snapshot(region.allocator(), .{ .operation = operation_id.? });
                    if (cut.records.len == 1 and cut.records[0].phase == .preparing) {
                        // Interrupted preparation may abort, but can never be
                        // promoted to a commit without its durable prepare cut.
                        const record = cut.records[0];
                        _ = try owner.authority().advance(&owner, region.allocator(), record, .{ .abort = try @import("distributed_txn.zig").commitVersion(record.identity.begin_timestamp) });
                    }
                    for (0..8) |_| {
                        var cursor: ?[16]u8 = null;
                        coordinator.advanceWork(&server, &cursor) catch |err| switch (err) {
                            error.MetadataMutationOutcomeUnknown => {},
                            else => return err,
                        };
                        coordinator.recoverSession(&server, txn_id, bytes.external_preparation.?, bytes.external_preparation_prepared, bytes.terminal_abort, bytes.terminal_commit != null) catch |err| switch (err) {
                            error.MetadataMutationOutcomeUnknown => {},
                            else => return err,
                        };
                    }
                    var terminal = (try durable.load(txn_id)).?;
                    defer terminal.deinit(a);
                    try std.testing.expect(terminal.terminal_abort or terminal.terminal_commit != null);
                    try std.testing.expect(terminal.external_receipt_acknowledged);
                    try std.testing.expect(terminal.execution_plan == null);
                    cut = try owner.snapshot(region.allocator(), .{ .operation = operation_id.? });
                    try std.testing.expectEqual(@as(usize, 0), cut.records.len);
                    try std.testing.expectEqual(@as(usize, 0), cut.receipts.len);
                }
            }
        }
    };
}

test "SQL command producer native scoped rollback retains its durable abort and releases only transaction locks" {
    const Coordinator = @import("sql_session.zig").Coordinator;
    const Adapter = @import("sql_session.zig").Adapter;
    const prepared = @import("sql_prepared_coordinator.zig");
    const Forward = struct {
        fn status(_: *anyopaque) anyerror!@import("../metadata/api.zig").MetadataStatus {
            return error.UnexpectedStatusRead;
        }
        fn catalog(raw: *anyopaque, alloc: A, _: local.api_operation.RequestContext, input: Call) anyerror![]u8 {
            return Native.call(raw, alloc, input);
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for (0..4) |mode| {
        const failed = mode & 1 != 0;
        const rejected = mode & 2 != 0;
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/scoped-rollback-{d}", .{ tmp.sub_path, mode });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var durable = transactions.DurableSessionStore.init(a, &metadata.store);
        var native: Native = .{ .metadata = &metadata, .standalone = true, .expiry_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 60000, .database = "default" };
        try setup(&metadata, &durable, true, &native);
        try native.until(&durable, .idle, 2);
        const ticket = try begin(&native, &durable, 60);
        const fence = try execute(&durable, ticket);
        var runtime = try local.storage_db_selected_root.db.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
        defer runtime.deinit();
        var server = @import("http_server.zig").ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr(), .session_store = &durable, .session_maintenance_local_only = true, .deployment_mode = .standalone }, .{ .ptr = &native, .vtable = &.{ .status = Forward.status, .system_catalog = Forward.catalog } }, null, null);
        defer server.deinit();
        var user: ?@import("http_server.zig").AuthenticatedIdentity = null;
        var coordinator: Coordinator = .{ .server = &server, .identity = &user, .context = .{ .sql_command_fence = &fence } };
        var adapter: Adapter = .{ .alloc = a, .registry = &server.txn_sessions, .node_id = 0, .commit_context = &coordinator, .commit_fn = Coordinator.commit, .rollback_fn = Coordinator.rollback, .connection_id = connection, .connection_revision = 1, .command_fence = &fence };
        const scope: local.sql_session.Scope = .{ .principal = "alice", .database = "default", .namespace = "public" };
        const transaction = try adapter.owner().vtable.begin(&adapter, scope, .{ .isolation = .read_committed });
        var source = (try producer.load(&durable, a, connection)).?;
        defer source.deinit();
        try native.submit(source.value, .{ .bind = .{ .command = ticket.command, .transaction = transaction.id } });
        try native.submit(source.value, .{ .effect = .{ .command = ticket.command, .effect = .{ .backend = fence.backend, .sequence = 1, .nonce = id(71), .action = .{ .try_acquire = .{ .resource = @splat(7), .mode = .{ .advisory = .exclusive }, .scope = .session } } } } });
        try native.submit(source.value, .{ .effect = .{ .command = ticket.command, .effect = .{ .backend = fence.backend, .sequence = 2, .nonce = id(72), .action = .{ .try_acquire = .{ .resource = @splat(8), .mode = .{ .advisory = .exclusive }, .scope = .{ .transaction = .{ .id = transaction.id, .subtransaction = 0 } } } } } } });
        if (failed) try server.txn_sessions.setSqlFailed(a, transaction.id, true);
        if (rejected) {
            native.failure = .admit;
            native.fail_before = true;
            native.block_cancel = true;
            try std.testing.expectError(error.SqlTransactionOutcomeUnknown, adapter.owner().vtable.rollback(&adapter, scope, transaction.id));
            var pending = (try durable.load(transaction.id)).?;
            defer pending.deinit(a);
            var seal = try std.json.parseFromSlice(transactions.ExternalPreparationSeal, a, pending.external_preparation.?, .{});
            defer seal.deinit();
            // Absence while this purpose can still arrive is NOT an abort
            // proof. The real native receiver must reject early lock release.
            try std.testing.expectError(error.CatalogPublicationProofPending, native.submit(source.value, .{ .release_unadmitted = try seal.value.admissionRequest() }));
            if (failed) {
                // Evict the admission window using definite rejected controls.
                // A lost cancellation ACK cannot strand cleanup after churn.
                for (0..authority.outcomes.retained + 1) |_| try std.testing.expectError(error.CatalogGenerationChanged, native.submit(source.value, .{ .bind = .{ .command = ticket.command, .transaction = id(999) } }));
            }
            native.block_cancel = false;
        } else try adapter.owner().vtable.rollback(&adapter, scope, transaction.id);
        var finishing = (try producer.load(&durable, a, connection)).?;
        defer finishing.deinit();
        try producer.finish(&durable, "alice", finishing.value, instance, ticket, try native.head(), native.nonce());
        try native.step(&durable, instance, 3);
        var waiting = (try producer.load(&durable, a, connection)).?;
        defer waiting.deinit();
        try std.testing.expectEqual(.finishing, waiting.value.phase);
        try std.testing.expect(!waiting.value.recovery);
        // The SQL outcome is terminal before ACK/reclamation; bounded native
        // cleanup can continue independently and must preserve session grants.
        for (0..8) |_| {
            var current = (try durable.load(transaction.id)).?;
            defer current.deinit(a);
            prepared.recoverSession(&server, transaction.id, current.external_preparation.?, current.external_preparation_prepared, current.terminal_abort, current.terminal_commit != null) catch |err| switch (err) {
                error.MetadataMutationOutcomeUnknown => {},
                else => return err,
            };
            var reconciled = try connections.loadReconciled(&durable, &server.txn_sessions, a, connection, "alice", 0, @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
            reconciled.deinit();
            var cursor: ?[16]u8 = null;
            prepared.advanceWork(&server, &cursor) catch |err| switch (err) {
                error.MetadataMutationOutcomeUnknown => {},
                else => return err,
            };
        }
        var terminal = (try durable.load(transaction.id)).?;
        defer terminal.deinit(a);
        try std.testing.expect(terminal.terminal_abort);
        try std.testing.expect(terminal.external_preparation != null);
        try std.testing.expect(terminal.execution_plan == null);
        var read = try metadata.store.beginReadTxn();
        defer read.abort();
        const ns: authority.scopes.ledger.Namespace = .{ .home = .metadata, .group = group, .incarnation = fence.backend.incarnation };
        const lock_owner = (try authority.scopes.ledger.readOwner(a, &read, ns, fence.backend)).?;
        try std.testing.expectEqual(@as(u32, 1), lock_owner.resources);
        try std.testing.expectEqual(@as(u32, 0), lock_owner.transaction_resources);
        try std.testing.expect(lock_owner.transaction == null);
        const native_scope = (try authority.scopes.read(a, &read, ns, fence.backend)).?;
        try std.testing.expect(native_scope.transaction == null);
        try std.testing.expectEqual(.active, native_scope.state);
        var reusable = try connections.load(&durable, a, connection, "alice", 0, @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
        defer reusable.deinit();
        try std.testing.expectEqual(.idle, reusable.value.state);
        // Source admission advances only after the native decision released
        // the command. It must preserve the backend and its session grants.
        try native.until(&durable, .idle, 3);
    }
}

fn advisoryFrame(source: *transactions.DurableSessionStore, fence: authority.scopes.CommandFence) @import("sql_advisory_execution.zig").Frame {
    return .{ .source = source, .connection = connection, .principal = "alice", .instance = instance, .fence = fence, .actor = actor };
}
fn finishBody(native: *Native, source: *transactions.DurableSessionStore, ticket: producer.Ticket) !void {
    var owned = (try producer.load(source, a, connection)).?;
    defer owned.deinit();
    try producer.finish(source, "alice", owned.value, instance, ticket, try native.head(), native.nonce());
}
fn finishMachine(native: *Native, machine: *@import("sql_advisory_execution.zig").Machine) !local.sql_scalar.Datum {
    for (0..128) |_| {
        const progress = machine.step(a, native.port(), 2, native.nonce()) catch |err| switch (err) {
            error.MetadataMutationOutcomeUnknown => continue,
            else => return err,
        };
        if (progress == .complete) return machine.value.?;
    }
    return error.AdvisoryMachineDidNotConverge;
}
fn nativeCut(native: *Native, backend: authority.locks.Backend, effect: ?authority.scopes.ledger.Effect) !std.json.Parsed(authority.Snapshot) {
    const bytes = try Native.call(native, a, .{ .sql_lock_read = .{ .incarnation = backend.incarnation, .standalone = native.standalone, .actor = actor, .database = 1, .backend = backend, .effect = effect } });
    defer a.free(bytes);
    return std.json.parseFromSlice(authority.Snapshot, a, bytes, .{ .allocate = .alloc_always });
}

test "SQL command producer native advisory effect delivery is exact across cold lost replies and key spaces" {
    const advisory = @import("sql_advisory_execution.zig");
    const Datum = local.sql_scalar.Datum;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |standalone| for ([_]bool{ false, true }) |replicated| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/advisory-driver-{any}-{any}", .{ tmp.sub_path, standalone, replicated });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        var native: Native = .{ .metadata = &metadata, .replicated = replicated };
        try setup(&metadata, &source, standalone, &native);
        try native.until(&source, .idle, 2);
        const ticket = try begin(&native, &source, 20);
        const fence = try execute(&source, ticket);
        var acquire = try advisory.Machine.init(advisoryFrame(&source, fence), .pg_try_advisory_lock, &.{Datum.json(.{ .integer = -42 })}, null, native.nonce());
        native.after = true;
        try std.testing.expectError(error.MetadataMutationOutcomeUnknown, acquire.step(a, native.port(), 2, native.nonce()));
        const exact = acquire.effect.?;
        metadata.deinit();
        metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        source = transactions.DurableSessionStore.init(a, &metadata.store);
        try std.testing.expect((try finishMachine(&native, &acquire)).value.bool);
        try std.testing.expect(std.meta.eql(exact, acquire.effect.?));
        const issued = native.calls;
        try std.testing.expectEqual(advisory.Machine.Progress.complete, try acquire.step(a, native.port(), 2, native.nonce()));
        try std.testing.expectEqual(issued, native.calls);
        var unlock = try advisory.Machine.init(advisoryFrame(&source, fence), .pg_advisory_unlock, &.{Datum.json(.{ .integer = -42 })}, null, native.nonce());
        try std.testing.expect((try finishMachine(&native, &unlock)).value.bool);
        var again = try advisory.Machine.init(advisoryFrame(&source, fence), .pg_advisory_unlock, &.{Datum.json(.{ .integer = -42 })}, null, native.nonce());
        try std.testing.expect(!(try finishMachine(&native, &again)).value.bool);
        var pair = try advisory.Machine.init(advisoryFrame(&source, fence), .pg_advisory_lock_shared, &.{ Datum.json(.{ .integer = -1 }), Datum.json(.{ .integer = -42 }) }, null, native.nonce());
        try std.testing.expect((try finishMachine(&native, &pair)).isVoid());
        try finishBody(&native, &source, ticket);
        try native.until(&source, .idle, 2);
        // A stale SQL execution cannot issue another grant after its native
        // command has completed, even when the exact backend remains alive.
        var stale = try advisory.Machine.init(advisoryFrame(&source, fence), .pg_advisory_lock, &.{Datum.json(.{ .integer = 99 })}, null, native.nonce());
        try std.testing.expectError(error.SqlConnectionBusy, stale.step(a, native.port(), 2, native.nonce()));
        var cut = try nativeCut(&native, fence.backend, pair.effect);
        defer cut.deinit();
        try std.testing.expectEqual(@as(u32, 1), cut.value.owner.?.resources);
        try std.testing.expectEqual(authority.scopes.ledger.Outcome.granted, cut.value.receipt.?.outcome);
    };
}

test "SQL command producer native completion cancels abandoned waiter across cold replies preserving prior session grants" {
    const advisory = @import("sql_advisory_execution.zig");
    const Datum = local.sql_scalar.Datum;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |standalone| for ([_]bool{ false, true }) |replicated| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/advisory-finish-{any}-{any}", .{ tmp.sub_path, standalone, replicated });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        var native: Native = .{ .metadata = &metadata, .replicated = replicated };
        try setup(&metadata, &source, standalone, &native);
        try native.until(&source, .idle, 2);
        const ticket = try begin(&native, &source, 20);
        const fence = try execute(&source, ticket);
        var owned = (try producer.load(&source, a, connection)).?;
        const baseline = owned.value;
        var holder = baseline;
        holder.registration.connection = @splat('b');
        holder.registration.lease.backend.id = id(50);
        holder.registration.nonce = id(51);
        const holder_command: authority.scopes.Command = .{ .sequence = 1, .nonce = id(52), .executor = id(53) };
        try native.submit(holder, .{ .register = holder.registration });
        try native.submit(holder, .{ .begin = holder_command });
        const held_resource = try authority.locks.advisoryResource(fence.backend.incarnation, 1, .{ .single = 42 });
        try native.submit(holder, .{ .effect = .{ .command = holder_command, .effect = .{ .backend = holder.registration.lease.backend, .sequence = 1, .nonce = id(54), .action = .{ .try_acquire = .{ .resource = held_resource, .mode = .{ .advisory = .exclusive }, .scope = .session } } } } });
        owned.deinit();
        var prior = try advisory.Machine.init(advisoryFrame(&source, fence), .pg_advisory_lock, &.{Datum.json(.{ .integer = 13 })}, null, native.nonce());
        _ = try finishMachine(&native, &prior);
        var queued = try advisory.Machine.init(advisoryFrame(&source, fence), .pg_advisory_lock, &.{Datum.json(.{ .integer = 42 })}, null, native.nonce());
        _ = try queued.step(a, native.port(), 2, native.nonce());
        {
            var cut = try nativeCut(&native, fence.backend, queued.effect);
            defer cut.deinit();
            try std.testing.expect(cut.value.owner.?.waiting != null);
        }
        try finishBody(&native, &source, ticket);
        // Lose one completion-control reply after native publication. Reopen
        // between every quantum; only native control, never SQL, resumes.
        native.after = true;
        for (0..32) |_| {
            if (try phase(&source) == .idle) break;
            native.step(&source, instance, 2) catch |err| switch (err) {
                error.MetadataMutationOutcomeUnknown => {},
                else => return err,
            };
            metadata.deinit();
            metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
            source = transactions.DurableSessionStore.init(a, &metadata.store);
        }
        try std.testing.expectEqual(producer.Phase.idle, try phase(&source));
        var cut = try nativeCut(&native, fence.backend, queued.effect);
        defer cut.deinit();
        try std.testing.expectEqual(authority.scopes.ledger.Outcome.cancelled, cut.value.receipt.?.outcome);
        try std.testing.expectEqual(@as(u32, 1), cut.value.owner.?.resources);
        try std.testing.expect(cut.value.owner.?.waiting == null and cut.value.owner.?.job == null);
        try std.testing.expectEqual(@as(@TypeOf(cut.value.scope.?.state), .active), cut.value.scope.?.state);
        try std.testing.expect(std.meta.eql(@as(?authority.scopes.Command, ticket.command), cut.value.scope.?.completed));
    };
}

test "SQL command producer native advisory SQL pipeline uses real receipts and bound transaction scope" {
    const advisory = @import("sql_advisory_execution.zig");
    const Hooks = struct {
        native: *Native,
        binds: usize = 0,
        transactions: usize = 0,
        pauses: usize = 0,
        checks: usize = 0,
        fn call(raw: *anyopaque, allocator: A, input: Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (input == .sql_lock_mutate) switch (input.sql_lock_mutate.operation) {
                .bind => self.binds += 1,
                .effect => |effect| switch (effect.effect.action) {
                    .acquire, .try_acquire => |acquire| if (acquire.scope == .transaction) {
                        try std.testing.expectEqual(@as(u64, 7), acquire.scope.transaction.subtransaction);
                        self.transactions += 1;
                    },
                    else => {},
                },
                else => {},
            };
            return Native.call(self.native, allocator, input);
        }
        fn now(_: *anyopaque) u64 {
            return 2;
        }
        fn nonce(raw: *anyopaque) ![16]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.native.nonce();
        }
        fn active(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks > 1024) return error.AdvisoryMachineDidNotConverge;
        }
        fn pause(raw: *anyopaque, _: u32) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.pauses += 1;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |standalone| for ([_]bool{ false, true }) |replicated| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/advisory-sql-{any}-{any}", .{ tmp.sub_path, standalone, replicated });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        var native: Native = .{ .metadata = &metadata, .replicated = replicated };
        try setup(&metadata, &source, standalone, &native);
        try native.until(&source, .idle, 2);
        const ticket = try begin(&native, &source, 20);
        const fence = try execute(&source, ticket);
        var hooks: Hooks = .{ .native = &native };
        var owner: advisory.Owner = .{
            .frame = advisoryFrame(&source, fence),
            .port = .{ .ptr = &hooks, .call = Hooks.call },
            .runtime = .{ .ptr = &hooks, .now_ms = Hooks.now, .nonce = Hooks.nonce, .active = Hooks.active, .pause = Hooks.pause },
            .transaction = .{ .id = id(60), .subtransaction = 7 },
        };
        const backend: local.sql_catalog.Backend = .{ .ptr = &hooks, .advisory_provider = owner.provider(), .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = Hooks.checkpoint } };
        var compiled = try local.sql_compiler.compile(a, "SELECT pg_try_advisory_xact_lock(42) AS x, pg_advisory_lock(13) AS s, pg_advisory_unlock(999) AS u, pg_advisory_unlock_all() AS a", .{});
        defer compiled.deinit();
        native.after = true; // Lose the exact transaction-bind reply.
        var result = try local.sql_runtime.execute(a, backend, &compiled, &.{}, .{ .native_result = true });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), hooks.binds);
        try std.testing.expectEqual(@as(usize, 1), hooks.transactions);
        try std.testing.expect(hooks.pauses > 0);
        const row = result.output.native_rows.?[0];
        try std.testing.expect(row[0].value.bool and row[1].isVoid() and !row[2].value.bool and row[3].isVoid());
        var cut = try nativeCut(&native, fence.backend, null);
        defer cut.deinit();
        try std.testing.expectEqual(@as(u32, 1), cut.value.owner.?.resources);
        try std.testing.expectEqual(@as(u32, 1), cut.value.owner.?.transaction_resources);
        try std.testing.expectEqualSlices(u8, &id(60), &cut.value.scope.?.transaction.?.id);
        try finishBody(&native, &source, ticket);
        try native.until(&source, .idle, 2);
    };
}

test "SQL command producer native completion finishes abandoned unlock all quanta without releasing transaction grants" {
    const advisory = @import("sql_advisory_execution.zig");
    const Datum = local.sql_scalar.Datum;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |standalone| for ([_]bool{ false, true }) |replicated| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/advisory-unlock-all-{any}-{any}", .{ tmp.sub_path, standalone, replicated });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        var native: Native = .{ .metadata = &metadata, .replicated = replicated };
        try setup(&metadata, &source, standalone, &native);
        try native.until(&source, .idle, 2);
        const ticket = try begin(&native, &source, 20);
        const fence = try execute(&source, ticket);
        for (1..5) |key_| {
            var acquire = try advisory.Machine.init(advisoryFrame(&source, fence), if (key_ == 4) .pg_advisory_xact_lock else .pg_advisory_lock, &.{Datum.json(.{ .integer = @intCast(key_) })}, .{ .id = id(60), .subtransaction = 7 }, native.nonce());
            _ = try finishMachine(&native, &acquire);
        }
        var release = try advisory.Machine.init(advisoryFrame(&source, fence), .pg_advisory_unlock_all, &.{}, null, native.nonce());
        _ = try release.step(a, native.port(), 2, native.nonce());
        {
            var cut = try nativeCut(&native, fence.backend, release.effect);
            defer cut.deinit();
            try std.testing.expect(cut.value.owner.?.job != null);
        }
        try finishBody(&native, &source, ticket);
        native.after = true;
        for (0..64) |_| {
            if (try phase(&source) == .idle) break;
            native.step(&source, instance, 2) catch |err| switch (err) {
                error.MetadataMutationOutcomeUnknown => {},
                else => return err,
            };
            metadata.deinit();
            metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
            source = transactions.DurableSessionStore.init(a, &metadata.store);
        }
        try std.testing.expectEqual(producer.Phase.idle, try phase(&source));
        var cut = try nativeCut(&native, fence.backend, release.effect);
        defer cut.deinit();
        try std.testing.expectEqual(authority.scopes.ledger.Outcome.completed, cut.value.receipt.?.outcome);
        try std.testing.expectEqual(@as(u32, 1), cut.value.owner.?.resources);
        try std.testing.expectEqual(@as(u32, 1), cut.value.owner.?.transaction_resources);
        try std.testing.expect(cut.value.owner.?.job == null and cut.value.owner.?.waiting == null);
    };
}

// These helpers use the same durable connection and producer contracts for a
// second SQL executor; the lock graph remains the actual metadata graph.
fn driveConnection(native: *Native, source: *transactions.DurableSessionStore, key: [32]u8, want: producer.Phase) !void {
    for (0..128) |_| {
        var owned = (try producer.load(source, a, key)).?;
        defer owned.deinit();
        if (owned.value.phase == want) return;
        try producer.advance(source, a, "alice", owned.value, instance, 2, native.nonce(), native.port());
    }
    return error.CommandProducerDidNotConverge;
}
fn beginConnection(native: *Native, source: *transactions.DurableSessionStore, key: [32]u8) !@import("sql_advisory_execution.zig").Frame {
    var owned = (try producer.load(source, a, key)).?;
    const ticket = try producer.begin(source, "alice", owned.value, instance, .{ .sequence = owned.value.next_command, .nonce = native.nonce(), .executor = native.nonce() }, @splat(8), try native.head(), native.nonce(), 2);
    owned.deinit();
    try driveConnection(native, source, key, .ready);
    owned = (try producer.load(source, a, key)).?;
    defer owned.deinit();
    const fence = try producer.take(source, "alice", owned.value, instance, ticket, 2);
    return .{ .source = source, .connection = key, .principal = "alice", .instance = instance, .fence = fence, .actor = actor };
}

test "SQL command producer native advisory contention avoids journal churn and hard deadlocks retain earlier grants" {
    const advisory = @import("sql_advisory_execution.zig");
    const Datum = local.sql_scalar.Datum;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |standalone| for ([_]bool{ false, true }) |replicated| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/advisory-deadlock-{any}-{any}", .{ tmp.sub_path, standalone, replicated });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        var native: Native = .{ .metadata = &metadata, .replicated = replicated };
        try setup(&metadata, &source, standalone, &native);
        try native.until(&source, .idle, 2);
        const key_b: [32]u8 = @splat('b');
        {
            var owned = (try producer.load(&source, a, connection)).?;
            defer owned.deinit();
            var registration = owned.value.registration;
            registration.connection = key_b;
            registration.lease.backend.id = id(50);
            registration.nonce = native.nonce();
            _ = try connections.mutate(&source, .{ .create = .{ .id = key_b, .principal = "alice", .owner_node_id = registration.lease.node, .expires_at_ms = 1000, .database = "main", .namespace = "public" } }, "alice", registration.lease.node, 1);
            try producer.create(&source, "alice", registration, actor, instance, try native.head(), native.nonce(), 1);
        }
        try driveConnection(&native, &source, key_b, .idle);
        const frame_a = try beginConnection(&native, &source, connection);
        var frame_b = try beginConnection(&native, &source, key_b);
        var first = try advisory.Machine.init(frame_a, .pg_advisory_lock, &.{Datum.json(.{ .integer = 1 })}, null, native.nonce());
        const before_apply = native.calls;
        const before_outcome = native.outcome_reads;
        const before_read = native.scope_reads;
        try std.testing.expectEqual(advisory.Machine.Progress.complete, try first.step(a, native.port(), 2, native.nonce()));
        // An uncontended grant returns its exact receipt in the accepting RPC:
        // one observation, one apply, no guaranteed-empty outcome lookup.
        try std.testing.expectEqual(before_apply + 1, native.calls);
        try std.testing.expectEqual(before_read + 1, native.scope_reads);
        try std.testing.expectEqual(before_outcome, native.outcome_reads);
        var second = try advisory.Machine.init(frame_b, .pg_advisory_lock, &.{Datum.json(.{ .integer = 2 })}, null, native.nonce());
        _ = try finishMachine(&native, &second);
        var waiter = try advisory.Machine.init(frame_a, .pg_advisory_lock, &.{Datum.json(.{ .integer = 2 })}, null, native.nonce());
        try std.testing.expectEqual(advisory.Machine.Progress.waiting, try waiter.step(a, native.port(), 2, native.nonce()));
        const queued_head = try native.head();
        const queued_applies = native.calls;
        for (0..64) |_| try std.testing.expectEqual(advisory.Machine.Progress.waiting, try waiter.step(a, native.port(), 2, native.nonce()));
        try std.testing.expectEqual(queued_applies, native.calls);
        try std.testing.expectEqual(queued_head, try native.head());
        var cycle = try advisory.Machine.init(frame_b, .pg_advisory_lock, &.{Datum.json(.{ .integer = 1 })}, null, native.nonce());
        try std.testing.expectError(error.SqlDeadlockDetected, finishMachine(&native, &cycle));
        {
            var cut = try nativeCut(&native, frame_b.fence.backend, cycle.effect);
            defer cut.deinit();
            try std.testing.expectEqual(authority.scopes.ledger.Outcome.deadlocked, cut.value.receipt.?.outcome);
            try std.testing.expectEqual(@as(u32, 1), cut.value.owner.?.resources);
            try std.testing.expect(cut.value.owner.?.waiting == null);
        }
        // Failed statement completion preserves session locks; a subsequent
        // command releases the conflict and the original waiter wakes.
        {
            var owned = (try producer.load(&source, a, key_b)).?;
            defer owned.deinit();
            try producer.finish(&source, "alice", owned.value, instance, owned.value.ticket.?, try native.head(), native.nonce());
        }
        try driveConnection(&native, &source, key_b, .idle);
        frame_b = try beginConnection(&native, &source, key_b);
        var unlock = try advisory.Machine.init(frame_b, .pg_advisory_unlock, &.{Datum.json(.{ .integer = 2 })}, null, native.nonce());
        try std.testing.expect((try finishMachine(&native, &unlock)).value.bool);
        try std.testing.expect((try finishMachine(&native, &waiter)).isVoid());
        var cut = try nativeCut(&native, frame_a.fence.backend, waiter.effect);
        defer cut.deinit();
        try std.testing.expectEqual(@as(u32, 2), cut.value.owner.?.resources);
        try std.testing.expectEqual(authority.scopes.ledger.Outcome.granted, cut.value.receipt.?.outcome);
    };
}

test "SQL command producer native advisory complete overload matrix retains scopes through uncertain delivery" {
    const advisory = @import("sql_advisory_execution.zig");
    const Datum = local.sql_scalar.Datum;
    const functions = [_]local.sql_advisory_effects.Function{
        .pg_advisory_lock,          .pg_advisory_lock_shared,
        .pg_try_advisory_lock,      .pg_try_advisory_lock_shared,
        .pg_advisory_unlock,        .pg_advisory_unlock_shared,
        .pg_advisory_xact_lock,     .pg_advisory_xact_lock_shared,
        .pg_try_advisory_xact_lock, .pg_try_advisory_xact_lock_shared,
        .pg_advisory_unlock_all,
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |standalone| for ([_]bool{ false, true }) |replicated| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/advisory-overloads-{any}-{any}", .{ tmp.sub_path, standalone, replicated });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        var native: Native = .{ .metadata = &metadata, .replicated = replicated };
        try setup(&metadata, &source, standalone, &native);
        try native.until(&source, .idle, 2);
        const ticket = try begin(&native, &source, 20);
        const frame = advisoryFrame(&source, try execute(&source, ticket));
        for (functions, 0..) |function, ordinal| for (1..3) |arity| {
            if (function == .pg_advisory_unlock_all and arity == 2) continue;
            const args = [_]Datum{ Datum.json(.{ .integer = -2147483648 }), Datum.json(.{ .integer = @intCast(ordinal) }) };
            // Separate resources for each overload/function; two-int keys and
            // bigint keys intentionally share numeric inputs without aliasing.
            const single = [_]Datum{Datum.json(.{ .integer = std.math.minInt(i64) + @as(i64, @intCast(ordinal)) })};
            const actual: []const Datum = if (function == .pg_advisory_unlock_all) &.{} else if (arity == 1) &single else &args;
            if (function.unlocks()) {
                var held = try advisory.Machine.init(frame, if (function.shared()) .pg_advisory_lock_shared else .pg_advisory_lock, actual, null, native.nonce());
                _ = try finishMachine(&native, &held);
            }
            var machine = try advisory.Machine.init(frame, function, actual, .{ .id = id(60), .subtransaction = 7 }, native.nonce());
            // Alternate uncertainty before and after native acceptance. The
            // exact effect remains stable; SQL is never evaluated again.
            native.before = arity == 1;
            native.after = arity == 2;
            const value = try finishMachine(&native, &machine);
            if (function.booleanResult()) try std.testing.expect(value.value.bool) else try std.testing.expect(value.isVoid());
            if (function.unlocks()) {
                var empty = try advisory.Machine.init(frame, function, actual, null, native.nonce());
                try std.testing.expect(!(try finishMachine(&native, &empty)).value.bool);
            }
        };
        var cut = try nativeCut(&native, frame.fence.backend, null);
        defer cut.deinit();
        try std.testing.expectEqual(@as(u32, 8), cut.value.owner.?.transaction_resources);
        try std.testing.expectEqual(@as(u32, 8), cut.value.owner.?.resources);
        try std.testing.expect(cut.value.owner.?.job == null and cut.value.owner.?.waiting == null);
        const writes = native.calls;
        var wrong_txn = try advisory.Machine.init(frame, .pg_advisory_xact_lock, &.{Datum.json(.{ .integer = 99 })}, .{ .id = id(61), .subtransaction = 0 }, native.nonce());
        try std.testing.expectError(error.InvalidLockScope, wrong_txn.step(a, native.port(), 2, native.nonce()));
        try std.testing.expectError(error.SqlNumericOutOfRange, advisory.Machine.init(frame, .pg_advisory_lock, &.{ Datum.json(.{ .integer = 2147483648 }), Datum.json(.{ .integer = 0 }) }, null, native.nonce()));
        try std.testing.expectError(error.SqlTypeMismatch, advisory.Machine.init(frame, .pg_advisory_lock, &.{Datum.voidValue()}, null, native.nonce()));
        try std.testing.expectError(error.SqlTransactionNotActive, advisory.Machine.init(frame, .pg_advisory_xact_lock, &.{Datum.json(.{ .integer = 1 })}, null, native.nonce()));
        try std.testing.expectEqual(writes, native.calls);
    };
}

test "SQL command producer native transaction savepoint projection retains monotone identities across cold rollback and release" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/advisory-savepoints", .{tmp.sub_path});
    defer a.free(path);
    var txn_id: [16]u8 = undefined;
    const expected = [_]u64{ 0, 1, 2, 1, 3, 1, 0, 4 };
    for (expected, 0..) |want, pass| {
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        var registry = transactions.SessionRegistry.init(&source);
        defer registry.deinit(a);
        if (pass == 0) {
            txn_id = (try registry.beginForPrincipal(a, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 9, "alice")).txn_id;
        } else {
            // Exercise the cold projection before a mutation loads the session
            // into the facade, then verify its hot projection after mutation.
            var prior = (try registry.getSqlState(a, txn_id)).?;
            defer prior.deinit(a);
            try std.testing.expectEqual(expected[pass - 1], prior.current_subtransaction);
            if (pass == 3 or pass == 4 or pass == 5) {
                source.fail_writes_for_test = true;
                switch (pass) {
                    3 => try std.testing.expectError(error.InjectedSessionStoreFailure, registry.rollbackToSavepoint(a, txn_id, 1)),
                    4 => try std.testing.expectError(error.InjectedSessionStoreFailure, registry.createNamedSavepoint(a, txn_id, "failed")),
                    5 => try std.testing.expectError(error.InjectedSessionStoreFailure, registry.releaseNamedSavepoint(a, txn_id, "point")),
                    else => unreachable,
                }
                source.fail_writes_for_test = false;
                var unchanged = (try registry.getSqlState(a, txn_id)).?;
                defer unchanged.deinit(a);
                try std.testing.expectEqual(expected[pass - 1], unchanged.current_subtransaction);
            }
            switch (pass) {
                1, 2, 4, 7 => _ = (try registry.createNamedSavepoint(a, txn_id, "point")).?,
                3 => _ = (try registry.rollbackToSavepoint(a, txn_id, 1)).?,
                5 => _ = (try registry.releaseNamedSavepoint(a, txn_id, "point")).?,
                6 => _ = (try registry.releaseSavepoint(a, txn_id, 1)).?,
                else => unreachable,
            }
        }
        var state = (try registry.getSqlState(a, txn_id)).?;
        defer state.deinit(a);
        try std.testing.expectEqual(want, state.current_subtransaction);
    }
}

const OwnedControlPlan = struct {
    owned: std.json.Parsed(producer.Record),
    value: producer.ControlPlan,
    fn deinit(self: *@This()) void {
        self.owned.deinit();
    }
};
fn savepointPlan(native: *Native, source: *transactions.DurableSessionStore, txn: [16]u8) !OwnedControlPlan {
    var owned = (try producer.load(source, a, connection)).?;
    errdefer owned.deinit();
    return .{ .value = try producer.observeControlPlan(a, owned.value, instance, txn, 2, native.nonce(), native.port()), .owned = owned };
}
fn attachedTransaction(native: *Native, source: *transactions.DurableSessionStore, registry: *transactions.SessionRegistry, fence: authority.scopes.CommandFence) ![16]u8 {
    const node: u64 = if (native.standalone) 0 else 9;
    var live = try connections.load(source, a, connection, "alice", node, 2);
    defer live.deinit();
    const txn = id(60);
    _ = try connections.mutateWithCommand(source, .{ .begin = .{ .id = connection, .expected_revision = live.value.revision, .txn_id = txn } }, "alice", node, 2, &fence);
    _ = try registry.beginForPrincipalWithSettingsAndId(a, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, node, "alice", &.{}, connection, txn);
    _ = try connections.mutateWithCommand(source, .{ .bind = .{ .id = connection, .txn_id = txn } }, "alice", node, 2, &fence);
    return txn;
}
fn stageSavepointRow(registry: *transactions.SessionRegistry, txn: [16]u8, key: []const u8) !void {
    const body = try std.fmt.allocPrint(a, "{{\"table\":\"items\",\"key\":\"{s}\",\"document\":{{\"v\":1}}}}", .{key});
    defer a.free(body);
    var parsed = try transactions.parseStageWriteRequest(a, body);
    defer parsed.deinit(a);
    _ = (try registry.stage(a, txn, &parsed)).?;
}
fn savepointRows(registry: *transactions.SessionRegistry, txn: [16]u8) !usize {
    const durable = registry.durable orelse return error.MissingDurableSessionStore;
    var persisted = (try durable.load(txn)).?;
    defer persisted.deinit(durable.alloc);
    const pending = persisted.staged orelse return 0;
    return if (pending.tables.len == 0) 0 else pending.tables[0].batch.writes.len;
}
fn coldSavepointControl(metadata: *storage.RaftApplyStore, source: *transactions.DurableSessionStore, registry: *transactions.SessionRegistry, native: *Native, path: []const u8, finishing: bool) !void {
    for (0..128) |_| {
        var current = (try producer.load(source, a, connection)).?;
        const done = current.value.control_effect == null and current.value.phase == (if (finishing) producer.Phase.idle else producer.Phase.executing);
        current.deinit();
        if (done) return;
        native.step(source, instance, 2) catch |err| switch (err) {
            error.MetadataMutationOutcomeUnknown => {},
            else => return err,
        };
        registry.deinit(a);
        metadata.deinit();
        metadata.* = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        source.* = transactions.DurableSessionStore.init(a, &metadata.store);
        registry.* = transactions.SessionRegistry.init(source);
    }
    return error.SavepointControlDidNotConverge;
}

test "SQL command producer native savepoint publication atomically rewinds SQL and reparents locks through cold lost control replies" {
    const advisory = @import("sql_advisory_execution.zig");
    const Datum = local.sql_scalar.Datum;
    const CutFixture = struct {
        closed: usize = 0,
        fn open(_: *anyopaque, _: A, _: local.api_table_read_source.RelationalStatementScan) !local.api_table_read_source.RelationalReadView {
            return error.UnexpectedSnapshotRecapture;
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closed += 1;
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |standalone| for ([_]bool{ false, true }) |replicated| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/savepoint-handoff-{any}-{any}", .{ tmp.sub_path, standalone, replicated });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        var registry = transactions.SessionRegistry.init(&source);
        defer registry.deinit(a);
        var native: Native = .{ .metadata = &metadata, .replicated = replicated };
        try setup(&metadata, &source, standalone, &native);
        try native.until(&source, .idle, 2);
        var ticket = try begin(&native, &source, 20);
        var fence = try execute(&source, ticket);
        const txn = try attachedTransaction(&native, &source, &registry, fence);
        var cut_fixture: CutFixture = .{};
        const retained = try @import("sql_retained_snapshot.zig").Shared.create(a, .{ .ptr = &cut_fixture, .vtable = &.{ .open = CutFixture.open, .close = CutFixture.close } }, &registry.snapshot_retirement);
        {
            defer retained.release();
            try registry.installNativeSqlCut(a, txn, retained);
        }
        try stageSavepointRow(&registry, txn, "root");
        var root = try advisory.Machine.init(advisoryFrame(&source, fence), .pg_advisory_xact_lock, &.{Datum.json(.{ .integer = 0 })}, .{ .id = txn, .subtransaction = 0 }, native.nonce());
        _ = try finishMachine(&native, &root);
        for (1..3) |ordinal| {
            var plan = try savepointPlan(&native, &source, txn);
            defer plan.deinit();
            const point = (try registry.changeSqlSavepoint(a, txn, if (ordinal == 1) "outer" else "inner", .create, plan.value)).?;
            try std.testing.expectEqual(@as(u64, @intCast(ordinal)), point.savepoint_id);
            var lock = try advisory.Machine.init(advisoryFrame(&source, fence), .pg_advisory_xact_lock, &.{Datum.json(.{ .integer = @intCast(ordinal) })}, .{ .id = txn, .subtransaction = point.savepoint_id }, native.nonce());
            _ = try finishMachine(&native, &lock);
            try stageSavepointRow(&registry, txn, if (ordinal == 1) "outer" else "inner");
        }
        var session_lock = try advisory.Machine.init(advisoryFrame(&source, fence), .pg_advisory_lock, &.{Datum.json(.{ .integer = 3 })}, null, native.nonce());
        _ = try finishMachine(&native, &session_lock);
        try registry.setSqlFailed(a, txn, true);
        // Both sides of the local publication abort together. Neither an SQL
        // rewind without an intent nor an intent without its rewind escapes.
        for ([_]@FieldType(transactions.DurableSessionStore, "fail_savepoint_publication_for_test"){ .session, .source }) |failure| {
            var plan = try savepointPlan(&native, &source, txn);
            defer plan.deinit();
            source.fail_savepoint_publication_for_test = failure;
            try std.testing.expectError(error.InjectedSessionStoreFailure, registry.changeSqlSavepoint(a, txn, "outer", .rollback, plan.value));
            source.fail_savepoint_publication_for_test = null;
            var state = (try registry.getSqlState(a, txn)).?;
            defer state.deinit(a);
            try std.testing.expect(state.metadata.failed);
            try std.testing.expectEqual(@as(u64, 2), state.current_subtransaction);
            try std.testing.expectEqual(@as(usize, 2), state.savepoints);
            try std.testing.expectEqual(@as(usize, 3), try savepointRows(&registry, txn));
            var unchanged = (try producer.load(&source, a, connection)).?;
            defer unchanged.deinit();
            try std.testing.expect(unchanged.value.control_effect == null and unchanged.value.pending == null);
            try std.testing.expectError(error.SqlTransactionAborted, registry.nativeSqlCut(a, txn));
            // This is an ownership assertion on the live test facade, not a
            // request to execute a read on the intentionally failed session.
            try std.testing.expect(registry.sessions.get(txn).?.native_sql_cut.? == retained);
            try std.testing.expectEqual(@as(usize, 0), cut_fixture.closed);
        }
        try registry.setSqlFailed(a, txn, false);
        {
            var plan = try savepointPlan(&native, &source, txn);
            defer plan.deinit();
            _ = (try registry.changeSqlSavepoint(a, txn, "inner", .release, plan.value)).?;
            try std.testing.expectError(error.SqlConnectionChanged, registry.changeSqlSavepoint(a, txn, "outer", .rollback, plan.value));
        }
        // SQL release is durable before native reparenting begins. No public
        // legacy path may mutate savepoints through this in-flight owner.
        try std.testing.expectError(error.SqlConnectionBusy, registry.createNamedSavepoint(a, txn, "bypass"));
        try std.testing.expectError(error.SqlConnectionBusy, registry.rollbackToNamedSavepoint(a, txn, "outer"));
        try std.testing.expectError(error.SqlConnectionBusy, registry.releaseNamedSavepoint(a, txn, "outer"));
        {
            const pinned = try registry.nativeSqlCut(a, txn);
            defer if (pinned.cut) |cut| cut.release();
            try std.testing.expect(pinned.locked and pinned.cut.? == retained);
        }
        const finishing = replicated;
        {
            var current = (try producer.load(&source, a, connection)).?;
            defer current.deinit();
            try producer.renew(&source, "alice", current.value, instance, 2000, try native.head(), native.nonce(), 2);
            // A pending renewal must never admit another SQL effect while
            // committed savepoint control is still outstanding.
            try std.testing.expectError(error.SqlConnectionBusy, advisoryFrame(&source, fence).requireExecuting(a, 2));
        }
        if (finishing) try finishBody(&native, &source, ticket);
        native.before = !finishing;
        native.after = finishing;
        try coldSavepointControl(&metadata, &source, &registry, &native, path, finishing);
        try std.testing.expectEqual(@as(usize, 1), cut_fixture.closed);
        {
            const cold = try registry.nativeSqlCut(a, txn);
            try std.testing.expect(cold.locked and cold.cut == null);
        }
        {
            var renewed = (try producer.load(&source, a, connection)).?;
            defer renewed.deinit();
            try std.testing.expectEqual(@as(u64, 2000), renewed.value.registration.lease.expires_at_ms);
        }
        try std.testing.expectEqual(@as(usize, 3), try savepointRows(&registry, txn));
        if (finishing) {
            ticket = try begin(&native, &source, 40);
            fence = try execute(&source, ticket);
        }
        try registry.setSqlFailed(a, txn, true);
        {
            var plan = try savepointPlan(&native, &source, txn);
            defer plan.deinit();
            _ = (try registry.changeSqlSavepoint(a, txn, "outer", .rollback, plan.value)).?;
        }
        // Before any native cleanup, the actual persisted SQL state is already
        // rewound, while every old native grant is still held conservatively.
        {
            var cut = try nativeCut(&native, fence.backend, null);
            defer cut.deinit();
            try std.testing.expectEqual(@as(u32, 3), cut.value.owner.?.transaction_resources);
            var state = (try registry.getSqlState(a, txn)).?;
            defer state.deinit(a);
            try std.testing.expect(!state.metadata.failed);
            try std.testing.expectEqual(@as(u64, 1), state.current_subtransaction);
            try std.testing.expectEqual(@as(usize, 1), try savepointRows(&registry, txn));
        }
        native.after = true;
        try finishBody(&native, &source, ticket);
        try coldSavepointControl(&metadata, &source, &registry, &native, path, true);
        try std.testing.expectEqual(@as(usize, 1), try savepointRows(&registry, txn));
        var cut = try nativeCut(&native, fence.backend, null);
        defer cut.deinit();
        // RELEASE reparented inner grants to outer. ROLLBACK TO outer removes
        // both sets, preserves root grants and preserves the session lock.
        try std.testing.expectEqual(@as(u32, 1), cut.value.owner.?.transaction_resources);
        try std.testing.expectEqual(@as(u32, 2), cut.value.owner.?.resources);
        try std.testing.expect(cut.value.owner.?.job == null and cut.value.owner.?.waiting == null);
        try std.testing.expect(std.meta.eql(cut.value.scope.?.transaction.?.id, txn));
    };
}

test "SQL command producer native shared SQL savepoint adapter drains exact control without replaying SQL" {
    const Coordinator = @import("sql_session.zig").Coordinator;
    const Adapter = @import("sql_session.zig").Adapter;
    const advisory = @import("sql_advisory_execution.zig");
    const Datum = local.sql_scalar.Datum;
    const Forward = struct {
        native: *Native,
        source: *transactions.DurableSessionStore,
        raced: bool = false,
        fn status(_: *anyopaque) anyerror!@import("../metadata/api.zig").MetadataStatus {
            return error.UnexpectedStatusRead;
        }
        fn catalog(raw: *anyopaque, alloc: A, _: local.api_operation.RequestContext, input: Call) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const bytes = try Native.call(self.native, alloc, input);
            errdefer alloc.free(bytes);
            if (!self.raced and input == .sql_lock_mutate and input.sql_lock_mutate.operation == .effect and
                input.sql_lock_mutate.operation.effect.effect.action == .control and input.sql_lock_mutate.operation.effect.effect.action.control == .release_savepoint)
            {
                // A real worker wins the local source CAS after this caller's
                // native publication but before its source acknowledgement.
                self.raced = true;
                var current = (try producer.load(self.source, alloc, connection)).?;
                defer current.deinit();
                try producer.advance(self.source, alloc, "alice", current.value, instance, 2, self.native.nonce(), self.native.port());
            }
            return bytes;
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |replicated| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/savepoint-adapter-{any}", .{ tmp.sub_path, replicated });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        var native: Native = .{ .metadata = &metadata, .replicated = replicated, .standalone = true, .expiry_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 10000, .database = "default" };
        try setup(&metadata, &source, true, &native);
        try native.until(&source, .idle, 2);
        const ticket = try begin(&native, &source, 60);
        const fence = try execute(&source, ticket);
        var runtime = try local.storage_db_selected_root.db.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
        defer runtime.deinit();
        var forward: Forward = .{ .native = &native, .source = &source };
        var server = @import("http_server.zig").ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr(), .session_store = &source, .session_maintenance_local_only = true, .deployment_mode = .standalone }, .{ .ptr = &forward, .vtable = &.{ .status = Forward.status, .system_catalog = Forward.catalog } }, null, null);
        defer server.deinit();
        server.sql_connection_admission.instance = instance;
        var user: ?@import("http_server.zig").AuthenticatedIdentity = null;
        var coordinator: Coordinator = .{ .server = &server, .identity = &user, .context = .{ .sql_command_fence = &fence } };
        var adapter: Adapter = .{ .alloc = a, .registry = &server.txn_sessions, .node_id = 0, .commit_context = &coordinator, .commit_fn = Coordinator.commit, .rollback_fn = Coordinator.rollback, .savepoint_fn = Coordinator.savepoint, .connection_id = connection, .connection_revision = 1, .command_fence = &fence };
        const scope: local.sql_session.Scope = .{ .principal = "alice", .database = "default", .namespace = "public" };
        const txn = (try adapter.owner().vtable.begin(&adapter, scope, .{ .isolation = .read_committed })).id;
        try adapter.owner().vtable.savepoint(&adapter, scope, txn, "outer");
        for (1..3) |ordinal| {
            if (ordinal == 2) try adapter.owner().vtable.savepoint(&adapter, scope, txn, "inner");
            var lock = try advisory.Machine.init(advisoryFrame(&source, fence), .pg_advisory_xact_lock, &.{Datum.json(.{ .integer = @intCast(ordinal) })}, .{ .id = txn, .subtransaction = @intCast(ordinal) }, native.nonce());
            _ = try finishMachine(&native, &lock);
        }
        native.after = true;
        try adapter.owner().vtable.release(&adapter, scope, txn, "inner");
        try std.testing.expect(forward.raced);
        {
            var renewed = (try producer.load(&source, a, connection)).?;
            defer renewed.deinit();
            try std.testing.expect(renewed.value.registration.lease.expires_at_ms > native.expiry_ms);
        }
        native.before = true;
        try adapter.owner().vtable.rollback_to(&adapter, scope, txn, "outer");
        const before_missing = native.calls;
        try std.testing.expectError(error.SqlSavepointNotFound, adapter.owner().vtable.release(&adapter, scope, txn, "inner"));
        try std.testing.expectEqual(before_missing, native.calls);
        var state = (try server.txn_sessions.getSqlState(a, txn)).?;
        defer state.deinit(a);
        try std.testing.expectEqual(@as(u64, 1), state.current_subtransaction);
        try std.testing.expectEqual(@as(usize, 1), state.savepoints);
        var cut = try nativeCut(&native, fence.backend, null);
        defer cut.deinit();
        try std.testing.expectEqual(@as(u32, 0), cut.value.owner.?.transaction_resources);
        try std.testing.expect(cut.value.owner.?.job == null);
        try finishBody(&native, &source, ticket);
        try native.until(&source, .idle, 2);
    }
}

const IngressRuntime = struct {
    native: *Native,
    now_ms: u64 = 2,
    cancelled: bool = false,
    cancel_on_begin: bool = false,
    fn call(raw: *anyopaque, alloc: A, input: Call) anyerror![]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const cancel = self.cancel_on_begin and input == .sql_lock_mutate and input.sql_lock_mutate.operation == .begin;
        defer if (cancel) {
            self.cancelled = true;
        };
        return Native.call(self.native, alloc, input);
    }
    fn now(raw: *anyopaque) u64 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.now_ms;
    }
    fn nonce(raw: *anyopaque) ![16]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.cancelled) return error.Canceled;
        return self.native.nonce();
    }
    fn active(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.cancelled) return error.Canceled;
    }
    fn pause(_: *anyopaque, _: u32) !void {}
    fn runtime(self: *@This()) @import("sql_command_execution.zig").Runtime {
        return .{ .ptr = self, .now_ms = now, .nonce = nonce, .active = active, .pause = pause };
    }
};

test "SQL command producer native ingress discovers one database enrolls exact owners and never reissues a body" {
    const execution = @import("sql_command_execution.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |standalone| for ([_]bool{ false, true }) |replicated| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/ingress-owner-{any}-{any}", .{ tmp.sub_path, standalone, replicated });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        var native: Native = .{ .metadata = &metadata, .standalone = standalone, .replicated = replicated };
        try metadata.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = @splat('1') });
        if (standalone) try metadata.replaceStandaloneCatalog(group, try metadata.standaloneRevision(), &.{}, &.{}, "{\"epoch\":1,\"tables\":[],\"ranges\":[]}");
        const activation = try std.json.Stringify.valueAlloc(a, protocol.Activation{ .version = protocol.current_version, .incarnation = @splat('1'), .member_count = 1, .membership_fingerprint = @splat(7) }, .{});
        defer a.free(activation);
        try metadata.applyStandaloneCommand(group, .{ .activate_topology_protocol = activation });
        const node: u64 = if (standalone) 0 else 9;
        _ = try connections.mutate(&source, .{ .create = .{ .id = connection, .principal = "alice", .owner_node_id = node, .expires_at_ms = 1000, .database = "default", .namespace = "public" } }, "alice", node, 1);
        var hooks: IngressRuntime = .{ .native = &native };
        const owner: execution.Controller = .{ .store = &source, .connection = connection, .principal = "alice", .actor = actor, .instance = instance, .node = node, .standalone = standalone, .port = native.port(), .runtime = hooks.runtime() };
        native.after = true;
        try owner.enroll(a, "default");
        try std.testing.expectEqual(producer.Phase.idle, try phase(&source));
        const registered = native.calls;
        try owner.enroll(a, "default");
        try std.testing.expectEqual(registered, native.calls);
        var first = try owner.take(a, @splat(8));
        var pool: @import("sql_connection_admission.zig").Pool = .{};
        defer pool.deinit(a);
        const key = @import("sql_connection_admission.zig").Key.init(connection, "alice");
        var admitted = try pool.acquire(a, std.testing.io, key);
        defer admitted.release(std.testing.io);
        try std.testing.expect(!try pool.executing(std.testing.io, key, first.ticket.command));
        try first.bind(std.testing.io, &admitted);
        try std.testing.expect(try pool.executing(std.testing.io, key, first.ticket.command));
        try std.testing.expectError(error.SqlConnectionBusy, owner.take(a, @splat(8)));
        var foreign = owner;
        foreign.instance = id(999);
        try std.testing.expectError(error.SqlConnectionBusy, foreign.take(a, @splat(8)));
        {
            var renewing = (try producer.load(&source, a, connection)).?;
            defer renewing.deinit();
            try producer.renew(&source, "alice", renewing.value, instance, 80000, try native.head(), native.nonce(), 2);
        }
        // A cancelled request cannot generate more entropy, but completion
        // has reserved its nonce before taking the execution permit. Its
        // in-flight renewal remains exact even without savepoint control.
        var copied_first = first;
        var substituted = first;
        substituted.fence.backend.id = id(900);
        try std.testing.expectError(error.SqlConnectionChanged, substituted.finish(a));
        hooks.cancelled = true;
        try first.finish(a);
        try first.finish(a);
        try std.testing.expectEqual(producer.Phase.finishing, try phase(&source));
        hooks.cancelled = false;
        admitted.release(std.testing.io);
        try std.testing.expect(!try pool.executing(std.testing.io, key, first.ticket.command));
        // Reopen before the next ingress command. Only pending native control
        // resumes; the earlier body cannot be adopted by either facade.
        metadata.deinit();
        metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        source = transactions.DurableSessionStore.init(a, &metadata.store);
        var second = try owner.take(a, @splat(8));
        try std.testing.expectEqual(first.ticket.command.sequence + 1, second.ticket.command.sequence);
        try std.testing.expect(!std.meta.eql(first.ticket.command, second.ticket.command));
        const before_old_finish = native.calls;
        admitted = try pool.acquire(a, std.testing.io, key);
        try std.testing.expectError(error.SqlConnectionBusy, copied_first.bind(std.testing.io, &admitted));
        try std.testing.expect(!try pool.executing(std.testing.io, key, first.ticket.command));
        try second.bind(std.testing.io, &admitted);
        try copied_first.finish(a);
        try std.testing.expect(try pool.executing(std.testing.io, key, second.ticket.command));
        try std.testing.expectEqual(before_old_finish, native.calls);
        var still_executing = (try producer.load(&source, a, connection)).?;
        defer still_executing.deinit();
        try std.testing.expectEqualDeep(second.ticket, still_executing.value.ticket.?);
        try std.testing.expectEqual(producer.Phase.executing, still_executing.value.phase);
        try second.finish(a);
        try native.until(&source, .idle, 2);
        try owner.retire(a);
        try native.until(&source, .retired, 2);
        try std.testing.expectError(error.SqlConnectionNotFound, owner.take(a, @splat(8)));
    };
}

test "SQL command producer native ingress discovery is capability gated and cannot mix selectors or realms" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/ingress-discovery", .{tmp.sub_path});
    defer a.free(path);
    var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
    defer metadata.deinit();
    try metadata.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = @splat('1') });
    const query: authority.Query = .{ .incarnation = @splat(0), .standalone = false, .actor = actor, .database = 0, .discovery = "default" };
    const before = try std.json.Stringify.valueAlloc(a, protocol.Activation{ .version = authority.decoder_version - 1, .incarnation = @splat('1'), .member_count = 1, .membership_fingerprint = @splat(7) }, .{});
    defer a.free(before);
    try metadata.applyStandaloneCommand(group, .{ .activate_topology_protocol = before });
    try std.testing.expectError(error.TableTopologyProtocolUpgradeRequired, metadata.sqlLockRead(a, group, query, .{}));
    const current = try std.json.Stringify.valueAlloc(a, protocol.Activation{ .version = authority.decoder_version, .incarnation = @splat('1'), .member_count = 1, .membership_fingerprint = @splat(7) }, .{});
    defer a.free(current);
    try metadata.applyStandaloneCommand(group, .{ .activate_topology_protocol = current });
    const bytes = try metadata.sqlLockRead(a, group, query, .{});
    defer a.free(bytes);
    var cut = try std.json.parseFromSlice(authority.Snapshot, a, bytes, .{});
    defer cut.deinit();
    try std.testing.expectEqual(@as(?u64, 1), cut.value.database_id);
    try std.testing.expect(cut.value.scope == null and cut.value.owner == null and cut.value.next_work == null);
    var forged = query;
    forged.standalone = true;
    try std.testing.expectError(error.CatalogGenerationChanged, metadata.sqlLockRead(a, group, forged, .{}));
    forged = query;
    forged.work = .{ .observed_at_ms = 2 };
    try std.testing.expectError(error.InvalidCatalogMutation, forged.validate());
    forged = query;
    forged.database = 1;
    try std.testing.expectError(error.InvalidCatalogMutation, forged.validate());
    forged = query;
    forged.discovery = "missing";
    try std.testing.expectError(error.DatabaseNotFound, metadata.sqlLockRead(a, group, forged, .{}));
}

test "SQL command producer native ingress cancellation skips an untaken body without dropping existing session locks" {
    const execution = @import("sql_command_execution.zig");
    const advisory = @import("sql_advisory_execution.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |standalone| for ([_]bool{ false, true }) |replicated| for ([_]bool{ false, true }) |after| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/ingress-skipped-{any}-{any}-{any}", .{ tmp.sub_path, standalone, replicated, after });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        var native: Native = .{ .metadata = &metadata, .replicated = replicated };
        try setup(&metadata, &source, standalone, &native);
        try native.until(&source, .idle, 2);
        var hooks: IngressRuntime = .{ .native = &native };
        const owner: execution.Controller = .{ .store = &source, .connection = connection, .principal = "alice", .actor = actor, .instance = instance, .node = if (standalone) 0 else 9, .standalone = standalone, .port = .{ .ptr = &hooks, .call = IngressRuntime.call }, .runtime = hooks.runtime() };
        var first = try owner.take(a, @splat(8));
        var held = try advisory.Machine.init(advisoryFrame(&source, first.fence), .pg_advisory_lock, &.{local.sql_scalar.Datum.json(.{ .integer = 42 })}, null, native.nonce());
        _ = try finishMachine(&native, &held);
        try first.finish(a);
        try native.until(&source, .idle, 2);
        hooks.cancel_on_begin = true;
        native.before = !after;
        native.after = after;
        try std.testing.expectError(error.Canceled, owner.take(a, @splat(9)));
        {
            var skipped = (try producer.load(&source, a, connection)).?;
            defer skipped.deinit();
            try std.testing.expect(skipped.value.body_skipped and !skipped.value.recovery);
            try std.testing.expectEqual(producer.Phase.beginning, skipped.value.phase);
        }
        metadata.deinit();
        metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        source = transactions.DurableSessionStore.init(a, &metadata.store);
        hooks.cancel_on_begin = false;
        hooks.cancelled = false;
        // Fresh ingress can assist the frozen skip, but can never turn it
        // into a permission to evaluate the cancelled SQL body.
        var next = try owner.take(a, @splat(10));
        try std.testing.expectEqual(first.ticket.command.sequence + 2, next.ticket.command.sequence);
        var cut = try nativeCut(&native, next.fence.backend, held.effect);
        defer cut.deinit();
        try std.testing.expectEqual(authority.scopes.ledger.Outcome.granted, cut.value.receipt.?.outcome);
        try std.testing.expectEqual(@as(u32, 1), cut.value.owner.?.resources);
        try std.testing.expectEqual(@as(@TypeOf(cut.value.scope.?.state), .active), cut.value.scope.?.state);
        try next.finish(a);
        try native.until(&source, .idle, 2);
        var idle = (try producer.load(&source, a, connection)).?;
        defer idle.deinit();
        try std.testing.expect(!idle.value.body_skipped and !idle.value.recovery);
    };
}

test "SQL command producer native exact command heartbeat renews through cold lost replies and rejects replacement admissions" {
    const execution = @import("sql_command_execution.zig");
    const admission = @import("sql_connection_admission.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |standalone| for ([_]bool{ false, true }) |replicated| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/command-heartbeat-{any}-{any}", .{ tmp.sub_path, standalone, replicated });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        var native: Native = .{ .metadata = &metadata, .replicated = replicated, .expiry_ms = 60002 };
        try setup(&metadata, &source, standalone, &native);
        try native.until(&source, .idle, 2);
        var hooks: IngressRuntime = .{ .native = &native };
        const owner: execution.Controller = .{ .store = &source, .connection = connection, .principal = "alice", .actor = actor, .instance = instance, .node = if (standalone) 0 else 9, .standalone = standalone, .port = native.port(), .runtime = hooks.runtime() };
        var loan = try owner.take(a, @splat(8));
        var pool: admission.Pool = .{};
        defer pool.deinit(a);
        var lease = try pool.acquire(a, std.testing.io, .init(connection, "alice"));
        defer lease.release(std.testing.io);
        try std.testing.expectError(error.SqlConnectionBusy, loan.keepAlive(a, std.testing.io, &pool));
        try loan.bind(std.testing.io, &lease);
        const before = native.calls;
        const reads = native.scope_reads;
        try std.testing.expectEqual(@as(u64, 60002), try loan.keepAlive(a, std.testing.io, &pool));
        try std.testing.expectEqual(before, native.calls);
        try std.testing.expectEqual(reads, native.scope_reads);
        hooks.now_ms = 25000;
        native.after = true;
        try std.testing.expectError(error.MetadataMutationOutcomeUnknown, loan.keepAlive(a, std.testing.io, &pool));
        try std.testing.expectEqual(before + 1, native.calls);
        {
            var pending = (try producer.load(&source, a, connection)).?;
            defer pending.deinit();
            try std.testing.expectEqual(@as(u64, 85000), pending.value.pending.?.operation.renew.expires_at_ms);
            try std.testing.expectEqual(@as(u64, 60002), pending.value.registration.lease.expires_at_ms);
            try std.testing.expectEqualDeep(loan.ticket, pending.value.ticket.?);
        }
        metadata.deinit();
        metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        source = transactions.DurableSessionStore.init(a, &metadata.store);
        try std.testing.expectEqual(@as(u64, 85000), try loan.keepAlive(a, std.testing.io, &pool));
        try std.testing.expectEqual(before + 1, native.calls);
        var foreign = loan;
        foreign.controller.instance = id(900);
        try std.testing.expectError(error.SqlConnectionBusy, foreign.keepAlive(a, std.testing.io, &pool));
        lease.release(std.testing.io);
        lease = try pool.acquire(a, std.testing.io, .init(connection, "alice"));
        hooks.now_ms = 50000;
        try std.testing.expectError(error.SqlConnectionBusy, loan.keepAlive(a, std.testing.io, &pool));
        try std.testing.expectEqual(before + 1, native.calls);
        // An unbound newer admission cannot extend the abandoned command.
        try loan.finish(a);
        try native.until(&source, .idle, hooks.now_ms);
        var fresh = try owner.take(a, @splat(9));
        try fresh.bind(std.testing.io, &lease);
        try std.testing.expectError(error.SqlConnectionBusy, loan.keepAlive(a, std.testing.io, &pool));
        _ = try fresh.keepAlive(a, std.testing.io, &pool);
        _ = try fresh.keepAlive(a, std.testing.io, &pool);
        hooks.now_ms = 110001;
        try std.testing.expectError(error.SqlConnectionBusy, fresh.keepAlive(a, std.testing.io, &pool));
        var expired = (try producer.load(&source, a, connection)).?;
        defer expired.deinit();
        try std.testing.expect(expired.value.recovery);
        try std.testing.expectEqual(@as(u64, 110000), expired.value.registration.lease.expires_at_ms);
    };
}

test "SQL command producer native completion renews pending control without replaying SQL or dropping session grants" {
    const execution = @import("sql_command_execution.zig");
    const advisory = @import("sql_advisory_execution.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |standalone| for ([_]bool{ false, true }) |replicated| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/completion-heartbeat-{any}-{any}", .{ tmp.sub_path, standalone, replicated });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        var native: Native = .{ .metadata = &metadata, .replicated = replicated, .expiry_ms = 60002 };
        try setup(&metadata, &source, standalone, &native);
        try native.until(&source, .idle, 2);
        var hooks: IngressRuntime = .{ .native = &native };
        const owner: execution.Controller = .{ .store = &source, .connection = connection, .principal = "alice", .actor = actor, .instance = instance, .node = if (standalone) 0 else 9, .standalone = standalone, .port = native.port(), .runtime = hooks.runtime() };
        var loan = try owner.take(a, @splat(8));
        var held = try advisory.Machine.init(advisoryFrame(&source, loan.fence), .pg_advisory_lock, &.{local.sql_scalar.Datum.json(.{ .integer = 42 })}, null, native.nonce());
        _ = try finishMachine(&native, &held);
        try loan.finish(a);
        {
            var pending = (try producer.load(&source, a, connection)).?;
            defer pending.deinit();
            const before = native.calls;
            native.after = true;
            try std.testing.expectError(error.MetadataMutationOutcomeUnknown, producer.maintain(&source, a, pending.value, instance, 25000, native.nonce(), native.port(), .control));
            try std.testing.expectEqual(before + 1, native.calls);
        }
        metadata.deinit();
        metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        source = transactions.DurableSessionStore.init(a, &metadata.store);
        try native.until(&source, .idle, 25000);
        var idle = (try producer.load(&source, a, connection)).?;
        defer idle.deinit();
        try std.testing.expectEqual(@as(u64, 85000), idle.value.registration.lease.expires_at_ms);
        try std.testing.expectEqual(loan.ticket.command.sequence + 1, idle.value.next_command);
        try std.testing.expect(!idle.value.recovery);
        var cut = try nativeCut(&native, loan.fence.backend, held.effect);
        defer cut.deinit();
        try std.testing.expectEqual(@as(u32, 1), cut.value.owner.?.resources);
        try std.testing.expectEqual(authority.scopes.ledger.Outcome.granted, cut.value.receipt.?.outcome);
        const before = native.calls;
        try producer.maintain(&source, a, idle.value, id(999), 50000, native.nonce(), native.port(), .idle);
        try std.testing.expectEqual(before, native.calls);
    };
}

test "SQL command producer native supervisor renews only live local owners and leaves expired sources fenced" {
    const Server = @import("http_server.zig").ApiHttpServer;
    const Forward = struct {
        fn status(_: *anyopaque) anyerror!@import("../metadata/api.zig").MetadataStatus {
            return error.UnexpectedStatusRead;
        }
        fn catalog(raw: *anyopaque, alloc: A, context: local.api_operation.RequestContext, input: Call) anyerror![]u8 {
            try std.testing.expect(context.setting_admin);
            try std.testing.expect(context.sql_completion == null);
            try context.ensureActive();
            return Native.call(raw, alloc, input);
        }
    };
    const Mode = enum { idle, executing, unbound, foreign_idle, foreign_executing, finishing, expired_idle, expired_executing, fenced };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |replicated| for (std.enums.values(Mode)) |mode| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/supervisor-heartbeat-{any}-{s}", .{ tmp.sub_path, replicated, @tagName(mode) });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        const now_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
        const expired = mode == .expired_idle or mode == .expired_executing;
        var native: Native = .{ .metadata = &metadata, .replicated = replicated, .expiry_ms = if (expired) now_ms - 1 else now_ms + 30000 };
        try setup(&metadata, &source, true, &native);
        try native.until(&source, .idle, 2);
        const executing = mode == .executing or mode == .unbound or mode == .foreign_executing or mode == .expired_executing or mode == .fenced;
        var command: ?authority.scopes.Command = null;
        if (executing or mode == .finishing) {
            const ticket = try begin(&native, &source, 200);
            _ = try execute(&source, ticket);
            command = ticket.command;
            if (mode == .finishing) try finishBody(&native, &source, ticket);
        }
        var runtime = try local.storage_db_selected_root.db.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
        defer runtime.deinit();
        var server = Server.init(a, .{ .backend_runtime = runtime.ptr(), .session_store = &source, .session_maintenance_local_only = true, .deployment_mode = .standalone }, .{ .ptr = &native, .vtable = &.{ .status = Forward.status, .system_catalog = Forward.catalog } }, null, null);
        defer server.deinit();
        server.sql_connection_admission.instance = if (mode == .foreign_idle or mode == .foreign_executing) id(999) else instance;
        const io = server.embedding_provider_runtime.io;
        var lease = try server.sql_connection_admission.acquire(a, io, .init(connection, "alice"));
        defer lease.release(io);
        if (executing and mode != .unbound) try lease.bindCommand(io, command.?);
        if (mode == .fenced) {
            var live = (try producer.load(&source, a, connection)).?;
            defer live.deinit();
            try native.submit(live.value, .{ .fence = .close });
        }
        const before = native.calls;
        var cursor: ?[32]u8 = null;
        try std.testing.expect(try producer.advanceWork(&server, &cursor));
        const should_renew = mode == .idle or mode == .executing or mode == .finishing;
        try std.testing.expectEqual(before + @as(usize, if (should_renew) 1 else 0), native.calls);
        var observed = (try producer.load(&source, a, connection)).?;
        defer observed.deinit();
        if (should_renew) {
            try std.testing.expect(observed.value.pending.?.operation == .renew);
            try std.testing.expect(observed.value.pending.?.operation.renew.expires_at_ms > native.expiry_ms);
            // The next sweep acknowledges native proof without another write.
            cursor = null;
            try std.testing.expect(try producer.advanceWork(&server, &cursor));
            try std.testing.expectEqual(before + 1, native.calls);
            var renewed = (try producer.load(&source, a, connection)).?;
            defer renewed.deinit();
            try std.testing.expect(renewed.value.registration.lease.expires_at_ms > native.expiry_ms);
            try std.testing.expect(!renewed.value.recovery);
        } else {
            try std.testing.expectEqual(native.expiry_ms, observed.value.registration.lease.expires_at_ms);
            try std.testing.expectEqual(expired or mode == .fenced, observed.value.recovery);
        }
    };
}

fn prepareDiscardResources(source: *transactions.DurableSessionStore, node: u64, now_ms: u64, expires_at_ms: u64, fence: *const authority.scopes.CommandFence) !u64 {
    const prepared = @import("sql_prepared.zig");
    const other: [32]u8 = @splat('c');
    _ = try connections.mutate(source, .{ .create = .{ .id = other, .principal = "alice", .owner_node_id = node, .expires_at_ms = expires_at_ms, .database = "main", .namespace = "public" } }, "alice", node, now_ms);
    _ = try connections.mutateWithCommand(source, .{ .overlay = .{ .id = connection, .expected_revision = 1, .entries = &.{.{ .identity = .{ .id = 44, .generation = 1 }, .value = .{ .string = "before" } }} } }, "alice", node, now_ms, fence);
    const sequence: local.sql_sequence_session.Identity = .{ .incarnation = @splat(0x11), .id = 9 };
    _ = try connections.mutateWithCommand(source, .{ .sequence_state = .{ .id = connection, .generation = 1, .sequence_epoch = 1, .active_txn = null, .value = .{ .values = &.{.{ .identity = sequence, .value = 42 }}, .last = sequence } } }, "alice", node, now_ms, fence);
    var path: @import("../pgwire/search_path.zig").Path = .{};
    try path.append(try @import("../pgwire/search_path.zig").Namespace.init("before"));
    _ = try connections.mutateWithCommand(source, .{ .search_path = .{ .id = connection, .expected_revision = 3, .value = path } }, "alice", node, now_ms, fence);
    for ([_][32]u8{ @splat('b'), @splat('d') }, [_][32]u8{ connection, other }) |resource_id, owner| {
        var record = try connections.load(source, a, owner, "alice", node, now_ms);
        defer record.deinit();
        try prepared.create(source, .{ .id = resource_id, .principal = "alice", .owner_node_id = node, .expires_at_ms = expires_at_ms, .database = record.value.database, .namespace = "public", .statement = "SELECT 1", .connection_id = owner, .connection_generation = 1, .parameter_types = &.{}, .bindings = &.{} }, now_ms);
    }
    return 4;
}

fn requireDiscardState(source: *transactions.DurableSessionStore, node: u64, now_ms: u64, reset: bool) !void {
    const prepared = @import("sql_prepared.zig");
    var record = try connections.load(source, a, connection, "alice", node, now_ms);
    defer record.deinit();
    try std.testing.expectEqual(@as(u64, if (reset) 2 else 1), record.value.generation);
    try std.testing.expectEqual(@as(u64, if (reset) 2 else 1), record.value.sequence_epoch);
    try std.testing.expectEqual(@as(u64, if (reset) 5 else 4), record.value.revision);
    try std.testing.expectEqual(@as(usize, if (reset) 0 else 1), record.value.overlay.len);
    try std.testing.expectEqual(@as(usize, if (reset) 0 else 1), record.value.sequence_state.values.len);
    try std.testing.expectEqual(reset, record.value.lookup_path == null);
    const resource: [32]u8 = @splat('b');
    if (reset) {
        try std.testing.expectError(error.SqlPreparedNotFound, prepared.loadForConnection(source, a, &resource, "alice", node, now_ms, connection));
    } else {
        var own = try prepared.loadForConnection(source, a, &resource, "alice", node, now_ms, connection);
        defer own.deinit();
    }
    const other: [32]u8 = @splat('d');
    var retained = try prepared.loadForConnection(source, a, &other, "alice", node, now_ms, @splat('c'));
    defer retained.deinit();
}

test "SQL command producer native DISCARD atomically resets resources with resumable unlock intent across cold faults" {
    const advisory = @import("sql_advisory_execution.zig");
    const Datum = local.sql_scalar.Datum;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |standalone| for ([_]bool{ false, true }) |replicated| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/discard-control-{any}-{any}", .{ tmp.sub_path, standalone, replicated });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        var native: Native = .{ .metadata = &metadata, .replicated = replicated };
        try setup(&metadata, &source, standalone, &native);
        try native.until(&source, .idle, 2);
        const ticket = try begin(&native, &source, 200);
        const fence = try execute(&source, ticket);
        const node: u64 = if (standalone) 0 else 9;
        const revision = try prepareDiscardResources(&source, node, 2, 900, &fence);
        for (0..3) |ordinal| {
            var grant = try advisory.Machine.init(advisoryFrame(&source, fence), .pg_advisory_lock, &.{Datum.json(.{ .integer = if (ordinal == 2) 43 else 42 })}, null, native.nonce());
            _ = try finishMachine(&native, &grant);
        }
        for ([_]@FieldType(transactions.DurableSessionStore, "fail_connection_publication_for_test"){ .source, .resources, .connection }) |failure| {
            var prior = (try producer.load(&source, a, connection)).?;
            defer prior.deinit();
            const plan = try producer.observeControlPlan(a, prior.value, instance, null, 2, native.nonce(), native.port());
            source.fail_connection_publication_for_test = failure;
            try std.testing.expectError(error.InjectedSessionStoreFailure, connections.mutateWithCommand(&source, .{ .discard_scoped = .{ .id = connection, .expected_revision = revision, .plan = plan } }, "alice", node, 2, &fence));
            source.fail_connection_publication_for_test = null;
            try requireDiscardState(&source, node, 2, false);
            var unchanged = (try producer.load(&source, a, connection)).?;
            defer unchanged.deinit();
            try std.testing.expect(unchanged.value.control_effect == null and unchanged.value.pending == null);
        }
        {
            var prior = (try producer.load(&source, a, connection)).?;
            defer prior.deinit();
            const stale = try producer.observeControlPlan(a, prior.value, instance, null, 2, native.nonce(), native.port());
            try producer.renew(&source, "alice", prior.value, instance, 80000, try native.head(), native.nonce(), 2);
            try std.testing.expectError(error.SqlConnectionChanged, connections.mutateWithCommand(&source, .{ .discard_scoped = .{ .id = connection, .expected_revision = revision, .plan = stale } }, "alice", node, 2, &fence));
            try requireDiscardState(&source, node, 2, false);
        }
        for (0..8) |_| {
            var current = (try producer.load(&source, a, connection)).?;
            defer current.deinit();
            if (current.value.pending == null) break;
            try native.step(&source, instance, 2);
        }
        {
            var prior = (try producer.load(&source, a, connection)).?;
            defer prior.deinit();
            const plan = try producer.observeControlPlan(a, prior.value, instance, null, 2, native.nonce(), native.port());
            try std.testing.expectEqual(@as(usize, 1), try connections.mutateWithCommand(&source, .{ .discard_scoped = .{ .id = connection, .expected_revision = revision, .plan = plan } }, "alice", node, 2, &fence));
        }
        try requireDiscardState(&source, node, 2, true);
        try std.testing.expectError(error.SqlConnectionBusy, connections.mutateWithCommand(&source, .{ .discard_sequences = connection }, "alice", node, 2, &fence));
        {
            var cut = try nativeCut(&native, fence.backend, null);
            defer cut.deinit();
            try std.testing.expectEqual(@as(u32, 2), cut.value.owner.?.resources);
        }
        try finishBody(&native, &source, ticket);
        native.before = !replicated;
        native.after = replicated;
        for (0..64) |_| {
            if (try phase(&source) == .idle) break;
            native.step(&source, instance, 2) catch |err| switch (err) {
                error.MetadataMutationOutcomeUnknown => {},
                else => return err,
            };
            metadata.deinit();
            metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
            source = transactions.DurableSessionStore.init(a, &metadata.store);
        }
        try std.testing.expectEqual(producer.Phase.idle, try phase(&source));
        try requireDiscardState(&source, node, 2, true);
        var cut = try nativeCut(&native, fence.backend, null);
        defer cut.deinit();
        try std.testing.expectEqual(@as(u32, 0), cut.value.owner.?.resources);
        try std.testing.expectEqual(@as(@TypeOf(cut.value.scope.?.state), .active), cut.value.scope.?.state);
        const next = try begin(&native, &source, 300);
        const next_fence = try execute(&source, next);
        try std.testing.expect(next_fence.backend.eql(fence.backend));
        var reused = try advisory.Machine.init(advisoryFrame(&source, next_fence), .pg_advisory_lock, &.{Datum.json(.{ .integer = 42 })}, null, native.nonce());
        _ = try finishMachine(&native, &reused);
    };
}

test "SQL command producer native DISCARD settings adapter survives lost control replies and cancellation after reset" {
    const advisory = @import("sql_advisory_execution.zig");
    const http = @import("http_server.zig");
    const Forward = struct {
        native: *Native,
        cancel: *std.atomic.Value(bool),
        cancel_on_effect: bool,
        fn status(_: *anyopaque) anyerror!@import("../metadata/api.zig").MetadataStatus {
            return error.UnexpectedStatusRead;
        }
        fn catalog(raw: *anyopaque, alloc: A, context: local.api_operation.RequestContext, input: Call) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try context.ensureActive();
            const stop = self.cancel_on_effect and input == .sql_lock_mutate and input.sql_lock_mutate.operation == .effect;
            defer if (stop) {
                self.cancel.store(true, .release);
            };
            return Native.call(self.native, alloc, input);
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |replicated| for ([_]bool{ false, true }) |cancel| {
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/discard-settings-{any}-{any}", .{ tmp.sub_path, replicated, cancel });
        defer a.free(path);
        var metadata = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer metadata.deinit();
        var source = transactions.DurableSessionStore.init(a, &metadata.store);
        const now_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
        var native: Native = .{ .metadata = &metadata, .replicated = replicated, .expiry_ms = now_ms + 60000, .database = "default" };
        try setup(&metadata, &source, true, &native);
        try native.until(&source, .idle, 2);
        const ticket = try begin(&native, &source, 200);
        const fence = try execute(&source, ticket);
        const revision = try prepareDiscardResources(&source, 0, now_ms, now_ms + 30000, &fence);
        var grant = try advisory.Machine.init(advisoryFrame(&source, fence), .pg_advisory_lock, &.{local.sql_scalar.Datum.json(.{ .integer = 42 })}, null, native.nonce());
        _ = try finishMachine(&native, &grant);
        var runtime = try local.storage_db_selected_root.db.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
        defer runtime.deinit();
        var cancelled: std.atomic.Value(bool) = .init(false);
        var forward: Forward = .{ .native = &native, .cancel = &cancelled, .cancel_on_effect = cancel };
        var server = http.ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr(), .session_store = &source, .session_maintenance_local_only = true, .deployment_mode = .standalone }, .{ .ptr = &forward, .vtable = &.{ .status = Forward.status, .system_catalog = Forward.catalog } }, null, null);
        defer server.deinit();
        server.sql_connection_admission.instance = instance;
        var user: ?http.AuthenticatedIdentity = .{ .username = @constCast("alice") };
        var adapter: @import("sql_execution.zig").Adapter = .{ .server = &server, .identity = &user, .database = "default", .namespace = "public", .connection_id = connection, .connection_revision = revision, .context = .{ .sql_command_fence = &fence, .cancellation = .fromAtomic(&cancelled) } };
        native.after = true;
        if (cancel) {
            try std.testing.expectError(error.Canceled, @import("sql_http_settings.zig").execute(a, &adapter, .discard_all, &.{}));
            try finishBody(&native, &source, ticket);
            try native.until(&source, .idle, now_ms);
        } else {
            var result = try @import("sql_http_settings.zig").execute(a, &adapter, .discard_all, &.{});
            defer result.deinit();
            try std.testing.expectEqualStrings("DISCARD ALL", result.output.command_tag);
            var current = (try producer.load(&source, a, connection)).?;
            defer current.deinit();
            try std.testing.expect(current.value.control_effect == null and !current.value.recovery);
        }
        try requireDiscardState(&source, 0, now_ms, true);
        var cut = try nativeCut(&native, fence.backend, null);
        defer cut.deinit();
        try std.testing.expectEqual(@as(u32, 0), cut.value.owner.?.resources);
        try std.testing.expectEqual(@as(@TypeOf(cut.value.scope.?.state), .active), cut.value.scope.?.state);
    };
}
