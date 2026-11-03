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
    before: bool = false,
    after: bool = false,
    calls: usize = 0,
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
            .sql_lock_read => |query| self.metadata.sqlLockRead(alloc, group, query, .{ .setting_admin = true }),
            .sql_lock_outcome => |request| self.metadata.sqlLockOutcome(alloc, group, request),
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
    _ = try connections.mutate(source, .{ .create = .{ .id = connection, .principal = "alice", .owner_node_id = node, .expires_at_ms = 1000, .database = "main", .namespace = "public" } }, "alice", node, 1);
    const registration: authority.scopes.Registration = .{ .lease = .{ .backend = .{ .incarnation = @splat(0x11), .id = id(2), .fence = 1 }, .database = 1, .node = node, .principal = try authority.principalDigest(a, .bootstrap), .expires_at_ms = 1000 }, .connection = connection, .nonce = id(3), .standalone = standalone };
    try producer.create(source, "alice", registration, actor, instance, try native.head(), native.nonce(), 1);
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
