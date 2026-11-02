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

const std = @import("std");
const sources = @import("antfly_local_sources");
const transactions = sources.storage_transactions;
const admission = sources.common_read_admission;

test "controlled transaction execution admission native BEGIN participant sidecar preserves bytes and cleanup" {
    const long: [8193]u8 = @splat('x');
    var participants: [131][]const u8 = @splat("docs");
    participants[0] = &long;
    const size = 4 + 4 * participants.len + long.len + 4 * (participants.len - 1);
    const legacy = try std.testing.allocator.alloc(u8, size);
    defer std.testing.allocator.free(legacy);
    std.mem.writeInt(u32, legacy[0..4], participants.len, .little);
    var offset: usize = 4;
    for (participants) |participant| {
        std.mem.writeInt(u32, legacy[offset..][0..4], @intCast(participant.len), .little);
        offset += 4;
        @memcpy(legacy[offset .. offset + participant.len], participant);
        offset += participant.len;
    }
    const Scope = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
    };
    var scope: Scope = .{};
    const payload: admission.Payload = .{ .ptr = &scope, .vtable = &.{ .checkpoint = Scope.check } };
    const control = admission.Port.init(&payload);
    const Fixture = struct {
        fn encode(allocator: std.mem.Allocator, items: []const []const u8, original: admission.Port) !void {
            const bytes = try transactions.encodeParticipantListWithAdmission(allocator, items, original);
            defer allocator.free(bytes);
        }
        fn decode(allocator: std.mem.Allocator, bytes: []const u8, original: admission.Port) !void {
            const items = try transactions.decodeParticipantListWithAdmission(allocator, bytes, original);
            defer transactions.freeParticipantList(allocator, items);
        }
    };
    const encoded = try transactions.encodeParticipantListWithAdmission(std.testing.allocator, &participants, control);
    defer std.testing.allocator.free(encoded);
    try std.testing.expectEqualSlices(u8, legacy, encoded);
    const encode_checks = scope.checks;
    scope = .{};
    const decoded = try transactions.decodeParticipantListWithAdmission(std.testing.allocator, legacy, control);
    defer transactions.freeParticipantList(std.testing.allocator, decoded);
    const decode_checks = scope.checks;
    try std.testing.expectEqualDeep(participants[0..], @as([]const []const u8, decoded));
    scope = .{};
    try std.testing.expect(try transactions.participantListsEqualWithAdmission(decoded, &participants, control));
    const equal_checks = scope.checks;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..encode_checks + 1) |fail_at| {
            scope = .{ .fail_at = fail_at, .cause = cause };
            try std.testing.expectError(cause, Fixture.encode(std.testing.allocator, &participants, control));
        }
        for (1..decode_checks + 1) |fail_at| {
            scope = .{ .fail_at = fail_at, .cause = cause };
            try std.testing.expectError(cause, Fixture.decode(std.testing.allocator, legacy, control));
        }
        for (1..equal_checks + 1) |fail_at| {
            scope = .{ .fail_at = fail_at, .cause = cause };
            try std.testing.expectError(cause, transactions.participantListsEqualWithAdmission(decoded, &participants, control));
        }
    }
    scope = .{};
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.encode, .{ &participants, control });
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.decode, .{ legacy, control });
    scope = .{};
    participants[130] = "late mismatch";
    try std.testing.expect(!try transactions.participantListsEqualWithAdmission(decoded, &participants, control));
    try std.testing.expectError(error.InvalidTxnRecord, Fixture.decode(std.testing.allocator, legacy[0 .. legacy.len - 1], control));
    const empty = try transactions.encodeParticipantListWithAdmission(std.testing.allocator, &.{}, control);
    defer std.testing.allocator.free(empty);
    try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 0 }, empty);
    for ([_]anyerror{ error.NotFound, error.TxnNotFound, error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        var trace: transactions.BeginAdmissionTrace = .{};
        const traced = trace.bind(control).?;
        scope = .{ .fail_at = 1, .cause = cause };
        try std.testing.expectError(cause, traced.checkpoint());
        try std.testing.expectEqual(cause, trace.failure.?);
        scope = .{};
        try std.testing.expectError(cause, traced.checkpoint());
        try std.testing.expectEqual(@as(usize, 0), scope.checks);
    }
}

test "controlled transaction execution admission staged batch rejects before commit and preserves accepted acknowledgement" {
    const BatchAdmission = sources.storage_transaction_batch_admission;
    const Pair = struct { key: []const u8, value: []const u8 };
    const writes = [_]Pair{ .{ .key = "record", .value = "txn" }, .{ .key = "sidecar", .value = "participants" } };
    const deletes = [_][]const u8{"missing"};
    const Replay = struct { sequence: u64, payload: []const u8 };
    const replay: ?Replay = .{ .sequence = 7, .payload = "replay" };
    const Scope = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        expired: bool = false,
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.expired or self.checks == self.fail_at) return self.cause;
        }
    };
    const Model = struct {
        scope: *Scope,
        staged: usize = 0,
        committed: usize = 0,
        aborts: usize = 0,
        replayed: bool = false,
        expire_on_put: bool = false,
        expire_on_commit: bool = false,
        commit_error: ?anyerror = null,
        pub fn delete(_: *@This(), key: []const u8) !void {
            try std.testing.expectEqualStrings("missing", key);
            return error.NotFound;
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            try std.testing.expectEqualStrings(if (self.staged == 0) "record" else "sidecar", key);
            try std.testing.expectEqualStrings(if (self.staged == 0) "txn" else "participants", value);
            self.staged += 1;
            if (self.expire_on_put) self.scope.expired = true;
        }
        pub fn setReplayOpaque(self: *@This(), sequence: u64, payload: []const u8) !void {
            try std.testing.expectEqual(@as(u64, 7), sequence);
            try std.testing.expectEqualStrings("replay", payload);
            self.replayed = true;
        }
        pub fn commit(self: *@This()) !void {
            if (self.commit_error) |cause| return cause;
            self.committed = self.staged;
            if (self.expire_on_commit) self.scope.expired = true;
        }
        pub fn abort(self: *@This()) void {
            self.aborts += 1;
            self.staged = 0;
            self.replayed = false;
        }
    };
    const Fixture = struct {
        fn run(batch: *Model, entries: []const Pair, removed: []const []const u8, replay_entry: ?Replay, control: ?admission.Port) !void {
            errdefer batch.abort();
            try BatchAdmission.apply(batch, entries, removed, replay_entry, control);
        }
    };
    var scope: Scope = .{};
    const payload: admission.Payload = .{ .ptr = &scope, .vtable = &.{ .checkpoint = Scope.check } };
    const control = admission.Port.init(&payload);
    var model: Model = .{ .scope = &scope };
    try Fixture.run(&model, &writes, &deletes, replay, control);
    try std.testing.expectEqual(@as(usize, 2), model.committed);
    try std.testing.expectEqual(@as(usize, 0), model.aborts);
    try std.testing.expect(model.replayed);
    const checks = scope.checks;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged, error.NotFound }) |cause| {
        for (1..checks + 1) |fail_at| {
            scope = .{ .fail_at = fail_at, .cause = cause };
            model = .{ .scope = &scope };
            try std.testing.expectError(cause, Fixture.run(&model, &writes, &deletes, replay, control));
            try std.testing.expectEqual(@as(usize, 0), model.committed);
            try std.testing.expectEqual(@as(usize, 0), model.staged);
            try std.testing.expectEqual(@as(usize, 1), model.aborts);
        }
        scope = .{ .cause = cause };
        model = .{ .scope = &scope, .expire_on_put = true };
        try std.testing.expectError(cause, Fixture.run(&model, &writes, &deletes, replay, control));
        try std.testing.expectEqual(@as(usize, 0), model.committed);
        try std.testing.expectEqual(@as(usize, 1), model.aborts);
        scope = .{ .cause = cause };
        model = .{ .scope = &scope, .expire_on_commit = true };
        try Fixture.run(&model, &writes, &deletes, replay, control);
        try std.testing.expectEqual(checks, scope.checks);
        try std.testing.expectEqual(@as(usize, 2), model.committed);
        try std.testing.expectEqual(@as(usize, 0), model.aborts);
        try std.testing.expectError(cause, control.checkpoint());
    }
    scope = .{};
    model = .{ .scope = &scope, .commit_error = error.TestCommitFailure };
    try std.testing.expectError(error.TestCommitFailure, Fixture.run(&model, &writes, &deletes, replay, control));
    try std.testing.expectEqual(@as(usize, 1), model.aborts);
    scope = .{ .expired = true };
    model = .{ .scope = &scope };
    try Fixture.run(&model, &writes, &deletes, replay, null);
    try std.testing.expectEqual(@as(usize, 0), scope.checks);
    try std.testing.expectEqual(@as(usize, 2), model.committed);
}

test "controlled transaction execution admission real memory backend BEGIN rejection is atomic and idempotent" {
    const mem = sources.storage_mem_backend;
    const Scope = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
    };
    const long: [8193]u8 = @splat('x');
    var participants: [13][]const u8 = @splat("docs");
    participants[0] = &long;
    const id: transactions.TxnId = @splat(17);
    const Fixture = struct {
        fn begin(manager: *transactions.TxnManager, txn: transactions.TxnId, items: []const []const u8, control: ?admission.Port) !void {
            try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionWithAdmission(txn, 100, 90, items, true, true, control);
        }
        fn verify(manager: *transactions.TxnManager, txn: transactions.TxnId, items: []const []const u8) !void {
            try std.testing.expectEqual(transactions.TxnStatus.pending, try manager.getTransactionStatus(txn));
            try std.testing.expectEqual(@as(u64, 100), try manager.getCommitVersion(txn));
            const persisted = try manager.getParticipants(std.testing.allocator, txn);
            defer transactions.freeParticipantList(std.testing.allocator, persisted);
            try std.testing.expect(try transactions.participantListsEqualWithAdmission(persisted, items, null));
        }
        fn freshFailure(items: []const []const u8, txn: transactions.TxnId, control: admission.Port, cause: anyerror) !void {
            var backend = mem.Backend.init(std.testing.allocator, .{});
            defer backend.close();
            var runtime = try backend.runtimeStore(std.testing.allocator, .{});
            defer runtime.deinit();
            var manager = try transactions.TxnManager.init(std.testing.allocator, &runtime);
            defer manager.deinit();
            try std.testing.expectError(cause, begin(&manager, txn, items, control));
            try std.testing.expectError(error.TxnNotFound, manager.getTransactionStatus(txn));
            const absent = try manager.getParticipants(std.testing.allocator, txn);
            defer transactions.freeParticipantList(std.testing.allocator, absent);
            try std.testing.expectEqual(@as(usize, 0), absent.len);
            // Rejection leaves no poisoned batch or transaction identity.
            try begin(&manager, txn, items, null);
            try verify(&manager, txn, items);
        }
        fn allocations(failing: std.mem.Allocator, items: []const []const u8, txn: transactions.TxnId, control: admission.Port) !void {
            var backend = mem.Backend.init(std.testing.allocator, .{});
            defer backend.close();
            var runtime = try backend.runtimeStore(std.testing.allocator, .{});
            defer runtime.deinit();
            var manager = try transactions.TxnManager.init(std.testing.allocator, &runtime);
            defer manager.deinit();
            manager.alloc = failing;
            begin(&manager, txn, items, control) catch |err| {
                manager.alloc = std.testing.allocator;
                try std.testing.expectError(error.TxnNotFound, manager.getTransactionStatus(txn));
                const absent = try manager.getParticipants(std.testing.allocator, txn);
                defer transactions.freeParticipantList(std.testing.allocator, absent);
                try std.testing.expectEqual(@as(usize, 0), absent.len);
                return err;
            };
            manager.alloc = std.testing.allocator;
            try verify(&manager, txn, items);
        }
    };
    var scope: Scope = .{};
    const payload: admission.Payload = .{ .ptr = &scope, .vtable = &.{ .checkpoint = Scope.check } };
    const control = admission.Port.init(&payload);
    var backend = mem.Backend.init(std.testing.allocator, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(std.testing.allocator, .{});
    defer runtime.deinit();
    var manager = try transactions.TxnManager.init(std.testing.allocator, &runtime);
    defer manager.deinit();
    try Fixture.begin(&manager, id, &participants, control);
    const begin_checks = scope.checks;
    try std.testing.expect(begin_checks > 30);
    try Fixture.verify(&manager, id, &participants);
    scope = .{};
    try Fixture.begin(&manager, id, &participants, control);
    const retry_checks = scope.checks;
    try std.testing.expect(retry_checks > 30);
    try Fixture.verify(&manager, id, &participants);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged, error.NotFound, error.TxnNotFound }) |cause| {
        for (1..begin_checks + 1) |fail_at| {
            scope = .{ .fail_at = fail_at, .cause = cause };
            try Fixture.freshFailure(&participants, id, control, cause);
        }
        for (1..retry_checks + 1) |fail_at| {
            scope = .{ .fail_at = fail_at, .cause = cause };
            try std.testing.expectError(cause, Fixture.begin(&manager, id, &participants, control));
            try Fixture.verify(&manager, id, &participants);
        }
    }
    scope = .{};
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.allocations, .{ &participants, id, control });
    try std.testing.expectError(error.DecisionConflict, manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionWithAdmission(id, 101, 90, &participants, true, true, control));
    var changed = participants;
    changed[12] = "changed";
    try std.testing.expectError(error.DecisionConflict, Fixture.begin(&manager, id, &changed, control));
    try Fixture.verify(&manager, id, &participants);
}

test "controlled transaction persisted BEGIN replays accepted WAL and excludes rejected records" {
    const lsm = sources.storage_lsm_backend;
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const path = try std.fmt.allocPrint(allocator, "/tmp/antfly-controlled-begin-wal-{d}", .{@import("antfly_platform").time.monotonicNs()});
    defer allocator.free(path);
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    const Scope = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
    };
    const Fixture = struct {
        fn id(number: usize) transactions.TxnId {
            var value: transactions.TxnId = @splat(0);
            std.mem.writeInt(u64, value[0..8], @intCast(number), .little);
            return value;
        }
        fn begin(manager: *transactions.TxnManager, txn: transactions.TxnId, items: []const []const u8, control: admission.Port) !void {
            try manager.initTransactionWithParticipantsCreatedAtRoleAndRetentionWithAdmission(txn, 100, 90, items, true, true, control);
        }
        fn verify(manager: *transactions.TxnManager, txn: transactions.TxnId, items: []const []const u8) !void {
            try std.testing.expectEqual(transactions.TxnStatus.pending, try manager.getTransactionStatus(txn));
            try std.testing.expectEqual(@as(u64, 100), try manager.getCommitVersion(txn));
            const persisted = try manager.getParticipants(std.testing.allocator, txn);
            defer transactions.freeParticipantList(std.testing.allocator, persisted);
            try std.testing.expect(try transactions.participantListsEqualWithAdmission(persisted, items, null));
        }
        fn absent(manager: *transactions.TxnManager, txn: transactions.TxnId) !void {
            try std.testing.expectError(error.TxnNotFound, manager.getTransactionStatus(txn));
            const participants = try manager.getParticipants(std.testing.allocator, txn);
            defer transactions.freeParticipantList(std.testing.allocator, participants);
            try std.testing.expectEqual(@as(usize, 0), participants.len);
        }
    };
    const long: [8193]u8 = @splat('x');
    const participants = [_][]const u8{ &long, "docs", "other" };
    const options: lsm.Options = .{ .backend = .{ .durability = .full }, .flush_threshold_bytes = 64 * 1024 * 1024 };
    var scope: Scope = .{};
    const payload: admission.Payload = .{ .ptr = &scope, .vtable = &.{ .checkpoint = Scope.check } };
    const control = admission.Port.init(&payload);
    var rejected_count: usize = 0;
    {
        var backend = try lsm.Backend.open(allocator, path, options);
        var abandon = false;
        defer if (abandon) backend.abandonAfterCrash() else backend.close();
        var runtime = try backend.runtimeStore(allocator, .{});
        defer runtime.deinit();
        var manager = try transactions.TxnManager.init(allocator, &runtime);
        defer manager.deinit();
        try std.testing.expect(backend.options.wal_enabled and backend.options.wal_sync_on_commit);
        try Fixture.begin(&manager, Fixture.id(1), &participants, control);
        scope = .{};
        try Fixture.begin(&manager, Fixture.id(2), &participants, control);
        const checks = scope.checks;
        try std.testing.expect(checks > 10);
        for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged, error.NotFound, error.TxnNotFound }) |cause| for (1..checks + 1) |fail_at| {
            const txn = Fixture.id(100 + rejected_count);
            rejected_count += 1;
            scope = .{ .fail_at = fail_at, .cause = cause };
            try std.testing.expectError(cause, Fixture.begin(&manager, txn, &participants, control));
            try Fixture.absent(&manager, txn);
        };
        try Fixture.verify(&manager, Fixture.id(1), &participants);
        try Fixture.verify(&manager, Fixture.id(2), &participants);
        try std.testing.expectEqual(@as(usize, 0), backend.runs.count());
        abandon = true;
    }
    {
        var backend = try lsm.Backend.open(allocator, path, options);
        defer backend.close();
        try std.testing.expect(backend.snapshotOpenStats().wal_replay_records > 0);
        var runtime = try backend.runtimeStore(allocator, .{});
        defer runtime.deinit();
        var manager = try transactions.TxnManager.init(allocator, &runtime);
        defer manager.deinit();
        try Fixture.verify(&manager, Fixture.id(1), &participants);
        try Fixture.verify(&manager, Fixture.id(2), &participants);
        for (0..rejected_count) |index| try Fixture.absent(&manager, Fixture.id(100 + index));
        scope = .{};
        try Fixture.begin(&manager, Fixture.id(1), &participants, control);
        try Fixture.verify(&manager, Fixture.id(1), &participants);
    }
}

test "controlled transaction execution admission intent envelope preserves bytes and original cleanup" {
    const codec = sources.storage_transaction_intent_admission;
    const data: [8193]u8 = @splat('x');
    const row: [8193]u8 = @splat('r');
    const inputs = [_]transactions.WriteIntent{ .{ .key = "one", .value = &data }, .{ .key = "one", .value = &data, .prepared_row = &row }, .{ .key = "one", .value = null }, .{ .key = "one", .value = "", .prepared_row = "" } };
    const Scope = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
    };
    var scope: Scope = .{};
    const payload: admission.Payload = .{ .ptr = &scope, .vtable = &.{ .checkpoint = Scope.check } };
    const control = admission.Port.init(&payload);
    const Fixture = struct {
        fn run(allocator: std.mem.Allocator, intent: transactions.WriteIntent, original: admission.Port) !void {
            const bytes = try codec.encode(allocator, intent, original);
            defer allocator.free(bytes);
        }
    };
    for (inputs) |intent| {
        const header: usize = if (intent.value != null and intent.prepared_row != null) 5 else 1;
        const value_len = if (intent.value) |bytes| bytes.len else 0;
        const row_len = if (intent.value != null) if (intent.prepared_row) |bytes| bytes.len else 0 else 0;
        const expected = try std.testing.allocator.alloc(u8, header + value_len + row_len);
        defer std.testing.allocator.free(expected);
        expected[0] = if (intent.value == null) 1 else if (intent.prepared_row != null) 2 else 0;
        if (header == 5) std.mem.writeInt(u32, expected[1..5], @intCast(value_len), .little);
        if (intent.value) |bytes| @memcpy(expected[header..][0..value_len], bytes);
        if (row_len != 0) @memcpy(expected[header + value_len ..], intent.prepared_row.?);
        scope = .{};
        const actual = try codec.encode(std.testing.allocator, intent, control);
        defer std.testing.allocator.free(actual);
        try std.testing.expectEqualSlices(u8, expected, actual);
        const checks = scope.checks;
        for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| for (1..checks + 1) |fail_at| {
            scope = .{ .fail_at = fail_at, .cause = cause };
            try std.testing.expectError(cause, Fixture.run(std.testing.allocator, intent, control));
        };
        scope = .{};
        try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ intent, control });
    }
}

test "controlled transaction persisted PREPARE retains accepted intents across rejection and WAL replay" {
    const lsm = sources.storage_lsm_backend;
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const path = try std.fmt.allocPrint(allocator, "/tmp/antfly-controlled-prepare-wal-{d}", .{@import("antfly_platform").time.monotonicNs()});
    defer allocator.free(path);
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    const Scope = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
    };
    var key_data: [8193]u8 = @splat('k');
    for (&key_data, 0..) |*byte, index| if (index % 3 == 0) {
        byte.* = 0;
    };
    const data: [8193]u8 = @splat('x');
    const changed_data: [8193]u8 = @splat('y');
    const value = try std.fmt.allocPrint(allocator, "{{\"_edges\":[],\"literal\":\"{s}\\n\"}}", .{data});
    defer allocator.free(value);
    const changed_value = try std.fmt.allocPrint(allocator, "{{\"_edges\":[],\"literal\":\"{s}\\n\"}}", .{changed_data});
    defer allocator.free(changed_value);
    const intents = [_]transactions.WriteIntent{ .{ .key = &key_data, .value = value }, .{ .key = "other", .value = "old" }, .{ .key = "other", .value = "new" }, .{ .key = "deleted", .value = null } };
    var replacement = intents;
    replacement[0].value = changed_value;
    const id: transactions.TxnId = @splat(83);
    const Fixture = struct {
        fn prepare(manager: *transactions.TxnManager, txn: transactions.TxnId, writes: []const transactions.WriteIntent, original: admission.Port) !void {
            try manager.writeIntentsExtraBatchWithAdmission(txn, writes, &.{}, .{}, original);
        }
        fn verify(manager: *transactions.TxnManager, txn: transactions.TxnId, bytes: []const u8, key: []const u8) !void {
            var batch = try manager.collectIntentBatch(std.testing.allocator, txn);
            defer batch.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u64, 2), batch.revision);
            try std.testing.expectEqual(@as(usize, 2), batch.writes.len);
            try std.testing.expectEqual(@as(usize, 1), batch.deletes.len);
            try std.testing.expectEqualStrings("deleted", batch.deletes[0]);
            for (batch.writes) |write| {
                if (std.mem.eql(u8, write.key, key)) try std.testing.expectEqualSlices(u8, bytes, write.value) else {
                    try std.testing.expectEqualStrings("other", write.key);
                    try std.testing.expectEqualStrings("new", write.value);
                }
            }
            try std.testing.expectEqual(transactions.TxnStatus.pending, try manager.getTransactionStatus(txn));
        }
    };
    var scope: Scope = .{};
    const payload: admission.Payload = .{ .ptr = &scope, .vtable = &.{ .checkpoint = Scope.check } };
    const control = admission.Port.init(&payload);
    const options: lsm.Options = .{ .backend = .{ .durability = .full }, .flush_threshold = 1024, .flush_threshold_bytes = 64 * 1024 * 1024 };
    {
        var backend = try lsm.Backend.open(allocator, path, options);
        var abandon = false;
        defer if (abandon) backend.abandonAfterCrash() else backend.close();
        var runtime = try backend.runtimeStore(allocator, .{});
        defer runtime.deinit();
        var manager = try transactions.TxnManager.init(allocator, &runtime);
        defer manager.deinit();
        try manager.initTransaction(id, 100);
        try Fixture.prepare(&manager, id, &intents, control);
        scope = .{};
        try Fixture.prepare(&manager, id, &intents, control);
        const checks = scope.checks;
        try std.testing.expect(checks > 20);
        try Fixture.verify(&manager, id, value, &key_data);
        for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged, error.NotFound, error.TxnNotFound }) |cause| for (1..checks + 1) |fail_at| {
            scope = .{ .fail_at = fail_at, .cause = cause };
            try std.testing.expectError(cause, Fixture.prepare(&manager, id, &replacement, control));
            try Fixture.verify(&manager, id, value, &key_data);
        };
        try std.testing.expectEqual(@as(usize, 0), backend.runs.count());
        abandon = true;
    }
    {
        var backend = try lsm.Backend.open(allocator, path, options);
        defer backend.close();
        try std.testing.expect(backend.snapshotOpenStats().wal_replay_records > 0);
        var runtime = try backend.runtimeStore(allocator, .{});
        defer runtime.deinit();
        var manager = try transactions.TxnManager.init(allocator, &runtime);
        defer manager.deinit();
        try Fixture.verify(&manager, id, value, &key_data);
        const other: transactions.TxnId = @splat(84);
        try manager.initTransaction(other, 200);
        try std.testing.expectError(error.IntentConflict, manager.writeIntents(other, &intents, &.{}));
    }
}

test "controlled transaction execution admission retained JSON cost preserves legacy reservation and rejection" {
    const codec = sources.storage_transaction_intent_admission;
    const allocator = std.testing.allocator;
    const long: [8193]u8 = @splat('x');
    const special = try std.fmt.allocPrint(allocator, "{{\"_edges\":[],\"literal\":\"{s}\\n\\u000b\\u2028\\\\\",\"number\":1.000e+2}}", .{long});
    defer allocator.free(special);
    const boundary = try std.fmt.allocPrint(allocator, "{{\"literal\":\"{s}_embeddings\"}}", .{long[0..4090]});
    defer allocator.free(boundary);
    const Scope = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
    };
    const Fixture = struct {
        fn legacy(alloc: std.mem.Allocator, value: []const u8) !u64 {
            if (std.mem.indexOf(u8, value, "_edges") == null and std.mem.indexOf(u8, value, "_embeddings") == null and std.mem.indexOfScalar(u8, value, '\\') == null) return value.len;
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value, .{ .parse_numbers = false }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidTxnRecord;
            defer parsed.deinit();
            if (parsed.value != .object) return value.len;
            var buffer: [1024]u8 = undefined;
            var counter = std.Io.Writer.Discarding.init(&buffer);
            try std.json.Stringify.value(parsed.value, .{}, &counter.writer);
            return @max(value.len, counter.fullCount());
        }
        const NoResize = struct {
            backing: std.mem.Allocator,
            fn allocate(raw: *anyopaque, length: usize, alignment: std.mem.Alignment, address: usize) ?[*]u8 {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return self.backing.rawAlloc(length, alignment, address);
            }
            fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, address: usize) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.backing.rawFree(bytes, alignment, address);
            }
        };
        fn run(alloc: std.mem.Allocator, value: []const u8, control: admission.Port) !void {
            var stable: NoResize = .{ .backing = alloc };
            const stable_allocator: std.mem.Allocator = .{ .ptr = &stable, .vtable = &.{ .alloc = NoResize.allocate, .free = NoResize.free, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap } };
            _ = try codec.payloadBytes(stable_allocator, value, control);
        }
    };
    var scope: Scope = .{};
    const payload: admission.Payload = .{ .ptr = &scope, .vtable = &.{ .checkpoint = Scope.check } };
    const control = admission.Port.init(&payload);
    for ([_][]const u8{ &long, special, boundary, "{\"x\":\"\\n\"}", "[\"_edges\"]", "\"_embeddings\"", "invalid _edges", "{\"_edges\":}" }) |value| {
        scope = .{};
        if (Fixture.legacy(allocator, value)) |expected| {
            try std.testing.expectEqual(expected, try codec.payloadBytes(allocator, value, control));
            const checks = scope.checks;
            for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged, error.InvalidCharacter, error.InvalidTxnRecord }) |cause| for (1..checks + 1) |fail_at| {
                scope = .{ .fail_at = fail_at, .cause = cause };
                try std.testing.expectError(cause, codec.payloadBytes(allocator, value, control));
            };
            scope = .{};
            try std.testing.checkAllAllocationFailures(allocator, Fixture.run, .{ value, control });
            scope = .{};
            const intent: transactions.WriteIntent = .{ .key = "row", .value = value, .retained_artifact_bytes = 7 };
            try std.testing.expectEqual(expected + 23 + 7, try transactions.retainedIntentBytesWithAdmission(allocator, intent, control));
        } else |cause| {
            try std.testing.expectError(cause, codec.payloadBytes(allocator, value, control));
        }
    }
    var storage: [0]u8 = .{};
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    scope = .{};
    try std.testing.expectEqual(@as(u64, long.len), try codec.payloadBytes(fixed.allocator(), &long, control));
    const typed: transactions.WriteIntent = .{ .key = "row", .value = "invalid\\_edges", .prepared_row = "physical" };
    try std.testing.expectEqual(@as(u64, typed.prepared_row.?.len + 23), try transactions.retainedIntentBytesWithAdmission(fixed.allocator(), typed, control));
}

test "controlled transaction execution admission key copying and cost preserve binary bytes and original rejection" {
    const codec = sources.storage_transaction_intent_admission;
    const allocator = std.testing.allocator;
    var data: [8193]u8 = @splat('x');
    for (&data, 0..) |*byte, index| if (index % 3 == 0) {
        byte.* = 0;
    };
    const txn: transactions.TxnId = @splat(17);
    const parts = [_][]const u8{ "prefix", &txn, ":", &data };
    const expected = try std.mem.concat(allocator, u8, &parts);
    defer allocator.free(expected);
    const fields: [131][]const u8 = @splat("col");
    const intent: transactions.WriteIntent = .{ .key = &data, .value = "{}", .prepared_row = "physical", .json_null_fields = &fields };
    const Scope = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
    };
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator, components: []const []const u8, control: admission.Port) !void {
            const key = try codec.concatKey(alloc, components, control);
            defer alloc.free(key);
        }
    };
    var scope: Scope = .{};
    const payload: admission.Payload = .{ .ptr = &scope, .vtable = &.{ .checkpoint = Scope.check } };
    const control = admission.Port.init(&payload);
    const actual = try codec.concatKey(allocator, &parts, control);
    defer allocator.free(actual);
    try std.testing.expectEqualSlices(u8, expected, actual);
    const copy_checks = scope.checks;
    scope = .{};
    const encoded_length = sources.storage_internal_keys.encodedComponentLen(&data);
    try std.testing.expectEqual(encoded_length, try codec.encodedComponentBytes(&data, control));
    const length_checks = scope.checks;
    scope = .{};
    const expected_admission: u64 = 2 + 8 + fields.len * (3 + @sizeOf([]const u8)) + data.len * 16 + 4096;
    try std.testing.expectEqual(expected_admission, try transactions.intentAdmissionBytesWithAdmission(intent, control));
    const admission_checks = scope.checks;
    try std.testing.expectEqual(@as(u64, 16 + 2 + encoded_length + 8), try transactions.retainedIntentBytesWithAdmission(allocator, intent, control));
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..copy_checks + 1) |fail_at| {
            scope = .{ .fail_at = fail_at, .cause = cause };
            try std.testing.expectError(cause, codec.concatKey(allocator, &parts, control));
        }
        for (1..length_checks + 1) |fail_at| {
            scope = .{ .fail_at = fail_at, .cause = cause };
            try std.testing.expectError(cause, codec.encodedComponentBytes(&data, control));
        }
        for (1..admission_checks + 1) |fail_at| {
            scope = .{ .fail_at = fail_at, .cause = cause };
            try std.testing.expectError(cause, transactions.intentAdmissionBytesWithAdmission(intent, control));
        }
    }
    scope = .{};
    try std.testing.checkAllAllocationFailures(allocator, Fixture.run, .{ &parts, control });
    const empty = try codec.concatKey(allocator, &.{}, control);
    defer allocator.free(empty);
    try std.testing.expectEqual(@as(usize, 0), empty.len);
    try std.testing.expectEqual(@as(usize, 2), try codec.encodedComponentBytes("", control));
}

test "controlled transaction execution admission real shared read guard conflict preserves original rejection" {
    const allocator = std.testing.allocator;
    var key: [8193]u8 = @splat('k');
    for (&key, 0..) |*byte, index| if (index % 3 == 0) {
        byte.* = 0;
    };
    var backend = sources.storage_mem_backend.Backend.init(allocator, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(allocator, .{});
    defer runtime.deinit();
    var manager = try transactions.TxnManager.init(allocator, &runtime);
    defer manager.deinit();
    const reader: transactions.TxnId = @splat(52);
    try manager.initTransaction(reader, 100);
    try manager.writeIntents(reader, &.{}, &.{.{ .key = &key, .expected_version = 0 }});
    const intents = [_]transactions.WriteIntent{.{ .key = &key, .value = "new" }};
    const Scope = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
    };
    var scope: Scope = .{};
    const payload: admission.Payload = .{ .ptr = &scope, .vtable = &.{ .checkpoint = Scope.check } };
    const control = admission.Port.init(&payload);
    try manager.checkIntentConflictsWithAdmission(&intents, reader, control);
    const checks = scope.checks;
    try std.testing.expect(checks > 20);
    try std.testing.expectError(error.IntentConflict, manager.checkIntentConflictsWithAdmission(&intents, null, control));
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged, error.NotFound, error.TxnNotFound, error.IntentConflict }) |cause| for (1..checks + 1) |fail_at| {
        scope = .{ .fail_at = fail_at, .cause = cause };
        try std.testing.expectError(cause, manager.checkIntentConflictsWithAdmission(&intents, reader, control));
        try manager.checkIntentConflicts(&intents, reader);
        try std.testing.expectError(error.IntentConflict, manager.checkIntentConflicts(&intents, null));
    };
    const Fixture = struct {
        fn run(failing: std.mem.Allocator, owner: *transactions.TxnManager, writes: []const transactions.WriteIntent, txn: transactions.TxnId, original: admission.Port) !void {
            const previous = owner.alloc;
            owner.alloc = failing;
            defer owner.alloc = previous;
            try owner.checkIntentConflictsWithAdmission(writes, txn, original);
        }
    };
    scope = .{};
    try std.testing.checkAllAllocationFailures(allocator, Fixture.run, .{ &manager, &intents, reader, control });
    try manager.checkIntentConflicts(&intents, reader);
    try std.testing.expectError(error.IntentConflict, manager.checkIntentConflicts(&intents, null));
    const writer: transactions.TxnId = @splat(53);
    try manager.initTransaction(writer, 200);
    try manager.writeIntents(writer, &.{.{ .key = "owned", .value = "value" }}, &.{});
    const owned = [_]transactions.WriteIntent{.{ .key = "owned", .value = "next" }};
    scope = .{};
    try manager.checkIntentConflictsWithAdmission(&owned, writer, control);
    try std.testing.expectError(error.IntentConflict, manager.checkIntentConflictsWithAdmission(&owned, null, control));
}

test "controlled transaction execution admission real version and span predicates preserve decisions and rejection" {
    const allocator = std.testing.allocator;
    const codec = sources.storage_transaction_intent_admission;
    const keys = sources.storage_internal_keys;
    const tracking = sources.storage_range_protection;
    const catalog = sources.storage_db_table_catalog;
    var logical: [8193]u8 = @splat('k');
    for (&logical, 0..) |*byte, index| if (index % 3 == 0) {
        byte.* = 0;
    };
    const value: [8193]u8 = @splat('v');
    const doc_key = try keys.documentKeyAlloc(allocator, &logical);
    defer allocator.free(doc_key);
    const ttl_key = try keys.ttlKeyAlloc(allocator, &logical);
    defer allocator.free(ttl_key);
    const row_key = try keys.relationalRowKeyAlloc(allocator, &logical);
    defer allocator.free(row_key);
    for ([_]struct { kind: u8, expected: []const u8 }{ .{ .kind = keys.primary_kind, .expected = doc_key }, .{ .kind = keys.ttl_kind, .expected = ttl_key }, .{ .kind = keys.relational_row_kind, .expected = row_key } }) |entry| {
        const actual = try codec.userKey(allocator, &logical, entry.kind, null);
        defer allocator.free(actual);
        try std.testing.expectEqualSlices(u8, entry.expected, actual);
    }
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&value, &digest, .{});
    var version: [8]u8 = undefined;
    std.mem.writeInt(u64, &version, 42, .little);
    const meta_key = "\x00\x00__metadata__:relational_integrity:test";
    const span: [tracking.index_span_digest_bytes]u8 = @splat(7);
    const counter = tracking.indexCounterKey(span);
    var backend = sources.storage_mem_backend.Backend.init(allocator, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(allocator, .{});
    defer runtime.deinit();
    {
        var batch = try runtime.beginBatch();
        errdefer batch.abort();
        try batch.put(doc_key, &value);
        try batch.put(ttl_key, &version);
        const table = (catalog.Catalog{ .mode_initialized = true, .storage_mode = .document }).encode();
        try batch.put(catalog.key, &table);
        try batch.put(meta_key, &value);
        try batch.put(tracking.activation_key, tracking.activation_value);
        try batch.put(&counter, &version);
        try batch.commit();
    }
    var manager = try transactions.TxnManager.init(allocator, &runtime);
    defer manager.deinit();
    const Scope = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
    };
    var scope: Scope = .{};
    const payload: admission.Payload = .{ .ptr = &scope, .vtable = &.{ .checkpoint = Scope.check } };
    const control = admission.Port.init(&payload);
    const predicates = [_]transactions.VersionPredicate{ .{ .key = &logical, .expected_version = 42, .expected_content_digest = digest }, .{ .key = meta_key, .comparison = .exact_value, .expected_value = &value } };
    try manager.checkVersionPredicates(&predicates, null);
    try manager.checkVersionPredicatesWithAdmission(&predicates, null, control);
    const checks = scope.checks;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged, error.NotFound, error.TxnNotFound, error.VersionConflict }) |cause| for (1..checks + 1) |fail_at| {
        scope = .{ .fail_at = fail_at, .cause = cause };
        try std.testing.expectError(cause, manager.checkVersionPredicatesWithAdmission(&predicates, null, control));
        try manager.checkVersionPredicates(&predicates, null);
    };
    scope = .{};
    var wrong = predicates;
    wrong[0].expected_version = 43;
    try std.testing.expectError(error.VersionConflict, manager.checkVersionPredicatesWithAdmission(&wrong, null, control));
    wrong = predicates;
    wrong[0].expected_content_digest = @splat(0);
    try std.testing.expectError(error.VersionConflict, manager.checkVersionPredicatesWithAdmission(&wrong, null, control));
    try std.testing.expectError(error.UniqueConstraintViolation, manager.checkVersionPredicatesWithAdmission(&.{.{ .key = &logical, .unique_absence = true }}, null, control));
    const reader: transactions.TxnId = @splat(61);
    try manager.initTransaction(reader, 100);
    try manager.writeIntents(reader, &.{}, &.{.{ .key = &counter, .comparison = .exact_value, .expected_value = &version }});
    scope = .{};
    try manager.checkIndexSpanGuardConflictsWithAdmission(&.{span}, reader, control);
    const span_checks = scope.checks;
    try std.testing.expectError(error.IntentConflict, manager.checkIndexSpanGuardConflictsWithAdmission(&.{span}, null, control));
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged, error.NotFound, error.TxnNotFound }) |cause| for (1..span_checks + 1) |fail_at| {
        scope = .{ .fail_at = fail_at, .cause = cause };
        try std.testing.expectError(cause, manager.checkIndexSpanGuardConflictsWithAdmission(&.{span}, reader, control));
        try manager.checkIndexSpanGuardConflictsWithAdmission(&.{span}, reader, null);
        try std.testing.expectError(error.IntentConflict, manager.checkIndexSpanGuardConflictsWithAdmission(&.{span}, null, null));
    };
}

test "controlled transaction execution admission real read guard staging is atomic and preserves identity" {
    const allocator = std.testing.allocator;
    const Scope = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
    };
    const Fixture = struct {
        fn run(failing: std.mem.Allocator, original: admission.Port) !void {
            const backing = std.testing.allocator;
            var key: [8193]u8 = @splat('r');
            for (&key, 0..) |*byte, index| if (index % 3 == 0) {
                byte.* = 0;
            };
            const value: [8193]u8 = @splat('v');
            const metadata = "\x00\x00__metadata__:relational_integrity:staging";
            const txn: transactions.TxnId = @splat(71);
            var backend = sources.storage_mem_backend.Backend.init(backing, .{});
            defer backend.close();
            var runtime = try backend.runtimeStore(backing, .{});
            defer runtime.deinit();
            {
                var batch = try runtime.beginBatch();
                errdefer batch.abort();
                try batch.put(metadata, &value);
                try batch.commit();
            }
            var manager = try transactions.TxnManager.init(backing, &runtime);
            defer manager.deinit();
            try manager.initTransaction(txn, 100);
            const writes = [_]transactions.WriteIntent{.{ .key = "write", .value = "next" }};
            const predicates = [_]transactions.VersionPredicate{
                .{ .key = &key, .expected_version = 0 },
                .{ .key = &key, .expected_version = 0 },
                .{ .key = metadata, .comparison = .exact_value, .expected_value = &value },
                .{ .key = "write", .expected_version = 0 },
            };
            manager.alloc = failing;
            const failure: ?anyerror = blk: {
                manager.writeIntentsExtraBatchWithAdmission(txn, &writes, &predicates, .{}, original) catch |err| break :blk err;
                break :blk null;
            };
            manager.alloc = backing;
            if (failure) |err| {
                var batch = try manager.collectIntentBatch(backing, txn);
                defer batch.deinit(backing);
                try std.testing.expectEqual(@as(u64, 0), batch.revision);
                try std.testing.expectEqual(@as(usize, 0), batch.writes.len);
                try std.testing.expectEqual(@as(usize, 0), batch.deletes.len);
                try std.testing.expect(!try manager.hasSharedReadDependency(txn, &key));
                try std.testing.expect(!try manager.hasSharedReadDependency(txn, metadata));
                try manager.checkIntentConflicts(&.{.{ .key = &key, .value = "other" }}, null);
                var probe = try runtime.beginProbe();
                defer probe.abort();
                try std.testing.expectError(error.NotFound, probe.get("\x00\x00__txn_read_guard_count"));
                return err;
            }
            try std.testing.expect(try manager.hasSharedReadDependency(txn, &key));
            try std.testing.expect(try manager.hasSharedReadDependency(txn, metadata));
            try std.testing.expect(!try manager.hasSharedReadDependency(txn, "write"));
            try manager.checkIntentConflicts(&.{.{ .key = &key, .value = "other" }}, txn);
            try std.testing.expectError(error.IntentConflict, manager.checkIntentConflicts(&.{.{ .key = &key, .value = "other" }}, null));
            const member = try std.mem.concat(backing, u8, &.{ "\x00\x00__txn_read_members__:", &txn, metadata });
            defer backing.free(member);
            var identity: [33]u8 = undefined;
            identity[0] = 3;
            std.crypto.hash.Blake3.hash(&value, identity[1..33], .{});
            {
                var probe = try runtime.beginProbe();
                defer probe.abort();
                try std.testing.expectEqualSlices(u8, &identity, try probe.get(member));
                try std.testing.expectEqual(@as(u64, 2), std.mem.readInt(u64, (try probe.get("\x00\x00__txn_read_guard_count"))[0..8], .little));
            }
            // A repeated prepare reuses both identities without growing guards.
            try manager.writeIntents(txn, &writes, &predicates);
            var probe = try runtime.beginProbe();
            defer probe.abort();
            try std.testing.expectEqualSlices(u8, &identity, try probe.get(member));
            try std.testing.expectEqual(@as(u64, 2), std.mem.readInt(u64, (try probe.get("\x00\x00__txn_read_guard_count"))[0..8], .little));
        }
    };
    var scope: Scope = .{};
    const payload: admission.Payload = .{ .ptr = &scope, .vtable = &.{ .checkpoint = Scope.check } };
    const control = admission.Port.init(&payload);
    try Fixture.run(allocator, control);
    const checks = scope.checks;
    try std.testing.expect(checks > 30);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged, error.NotFound, error.TxnNotFound, error.VersionConflict }) |cause| for (1..checks + 1) |fail_at| {
        scope = .{ .fail_at = fail_at, .cause = cause };
        try std.testing.expectError(cause, Fixture.run(allocator, control));
    };
    scope = .{};
    try std.testing.checkAllAllocationFailures(allocator, Fixture.run, .{control});
}

test "controlled transaction execution admission indexed resolution acknowledgement remains visible and atomic" {
    const allocator = std.testing.allocator;
    var backend = sources.storage_mem_backend.Backend.init(allocator, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(allocator, .{});
    defer runtime.deinit();
    var manager = try transactions.TxnManager.init(allocator, &runtime);
    defer manager.deinit();
    const txn: transactions.TxnId = @splat(91);
    try manager.initTransactionWithParticipants(txn, 100, &.{ "local", "remote" });
    try manager.markParticipantsResolvedExtraBatch(txn, &.{"remote"}, .{});
    try manager.writeIntents(txn, &.{.{ .key = "document", .value = "{}" }}, &.{});
    _ = try manager.resolveIntentsWithExtraBatch(txn, .committed, 200, .{ .resolved_participant = "local" });
    const resolved = try manager.getResolvedParticipants(allocator, txn);
    defer transactions.freeParticipantList(allocator, resolved);
    try std.testing.expectEqual(@as(usize, 2), resolved.len);
    const unresolved = try manager.getUnresolvedParticipants(allocator, txn);
    defer transactions.freeParticipantList(allocator, unresolved);
    try std.testing.expectEqual(@as(usize, 0), unresolved.len);
    try std.testing.expectEqual(transactions.TxnStatus.committed, try manager.getTransactionStatus(txn));
    _ = try manager.resolveIntentsWithExtraBatch(txn, .committed, 200, .{ .resolved_participant = "local" });
    const repeated = try manager.getResolvedParticipants(allocator, txn);
    defer transactions.freeParticipantList(allocator, repeated);
    try std.testing.expectEqual(@as(usize, 2), repeated.len);
}

test "controlled transaction execution admission indexed resolution avoids cohort-sized preparation" {
    const Fixture = struct {
        fn bytes(count: usize) !usize {
            const backing = std.testing.allocator;
            const participants = try backing.alloc([]const u8, count + 2);
            defer backing.free(participants);
            participants[0] = "local";
            participants[1] = "remote";
            var initialized: usize = 2;
            defer for (participants[2..initialized]) |name| backing.free(name);
            for (participants[2..], 0..) |*name, i| {
                name.* = try std.fmt.allocPrint(backing, "member-{d}", .{i});
                initialized += 1;
            }
            var backend = sources.storage_mem_backend.Backend.init(backing, .{});
            defer backend.close();
            var runtime = try backend.runtimeStore(backing, .{});
            defer runtime.deinit();
            var manager = try transactions.TxnManager.init(backing, &runtime);
            defer manager.deinit();
            const txn: transactions.TxnId = @splat(92);
            try manager.initTransactionWithParticipants(txn, 100, participants);
            try manager.markParticipantsResolvedExtraBatch(txn, &.{"remote"}, .{});
            try manager.writeIntents(txn, &.{.{ .key = "document", .value = "{}" }}, &.{});
            var measured = std.testing.FailingAllocator.init(backing, .{});
            manager.alloc = measured.allocator();
            defer manager.alloc = backing;
            _ = try manager.resolveIntentsWithExtraBatch(txn, .committed, 200, .{ .resolved_participant = "local" });
            return measured.allocated_bytes;
        }
    };
    const small = try Fixture.bytes(16);
    const large = try Fixture.bytes(256);
    std.debug.print("indexed resolution preparation bytes: small={d} large={d}\n", .{ small, large });
    try std.testing.expect(large <= small + 1024);
}

test "controlled transaction execution admission indexed resolution allocation failures preserve pending decision" {
    const Fixture = struct {
        const NoResize = struct {
            inner: std.mem.Allocator,
            fn allocate(raw: *anyopaque, length: usize, alignment: std.mem.Alignment, address: usize) ?[*]u8 {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return self.inner.rawAlloc(length, alignment, address);
            }
            fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
                return false;
            }
            fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
                return null;
            }
            fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, address: usize) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.inner.rawFree(bytes, alignment, address);
            }
            fn allocator(self: *@This()) std.mem.Allocator {
                return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = resize, .remap = remap, .free = free } };
            }
        };
        fn run(failing: std.mem.Allocator) !void {
            const backing = std.testing.allocator;
            var backend = sources.storage_mem_backend.Backend.init(backing, .{});
            defer backend.close();
            var runtime = try backend.runtimeStore(backing, .{});
            defer runtime.deinit();
            var manager = try transactions.TxnManager.init(backing, &runtime);
            defer manager.deinit();
            const txn: transactions.TxnId = @splat(93);
            try manager.initTransactionWithParticipants(txn, 100, &.{ "local", "remote" });
            try manager.markParticipantsResolvedExtraBatch(txn, &.{"remote"}, .{});
            try manager.writeIntents(txn, &.{.{ .key = "document", .value = "{}" }}, &.{});
            var stable: NoResize = .{ .inner = failing };
            manager.alloc = stable.allocator();
            const failure: ?anyerror = blk: {
                _ = manager.resolveIntentsWithExtraBatch(txn, .committed, 200, .{ .resolved_participant = "local" }) catch |err| break :blk err;
                break :blk null;
            };
            manager.alloc = backing;
            const resolved = try manager.getResolvedParticipants(backing, txn);
            defer transactions.freeParticipantList(backing, resolved);
            if (failure) |err| {
                try std.testing.expectEqual(transactions.TxnStatus.pending, try manager.getTransactionStatus(txn));
                try std.testing.expectEqual(@as(usize, 1), resolved.len);
                try std.testing.expectEqualStrings("remote", resolved[0]);
                var intents = try manager.collectIntentBatch(backing, txn);
                defer intents.deinit(backing);
                try std.testing.expectEqual(@as(u64, 1), intents.revision);
                try std.testing.expectEqual(@as(usize, 1), intents.writes.len);
                return err;
            }
            try std.testing.expectEqual(transactions.TxnStatus.committed, try manager.getTransactionStatus(txn));
            try std.testing.expectEqual(@as(usize, 2), resolved.len);
            try std.testing.expectError(error.InvalidParticipant, manager.resolveIntentsWithExtraBatch(txn, .committed, 200, .{ .resolved_participant = "missing" }));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "controlled transaction persisted indexed resolution retains acknowledgements after WAL replay" {
    const lsm = sources.storage_lsm_backend;
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const path = try std.fmt.allocPrint(allocator, "/tmp/antfly-indexed-resolution-wal-{d}", .{@import("antfly_platform").time.monotonicNs()});
    defer allocator.free(path);
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    const txn: transactions.TxnId = @splat(94);
    const options: lsm.Options = .{ .backend = .{ .durability = .full }, .flush_threshold_bytes = 64 * 1024 * 1024 };
    {
        var backend = try lsm.Backend.open(allocator, path, options);
        var abandon = false;
        defer if (abandon) backend.abandonAfterCrash() else backend.close();
        var runtime = try backend.runtimeStore(allocator, .{});
        defer runtime.deinit();
        var manager = try transactions.TxnManager.init(allocator, &runtime);
        defer manager.deinit();
        try manager.initTransactionWithParticipants(txn, 100, &.{ "local", "remote" });
        try manager.markParticipantsResolvedExtraBatch(txn, &.{"remote"}, .{});
        try manager.writeIntents(txn, &.{.{ .key = "document", .value = "{}" }}, &.{});
        _ = try manager.resolveIntentsWithExtraBatch(txn, .committed, 200, .{ .resolved_participant = "local" });
        try std.testing.expect(backend.options.wal_enabled and backend.options.wal_sync_on_commit);
        try std.testing.expectEqual(@as(usize, 0), backend.runs.count());
        abandon = true;
    }
    {
        var backend = try lsm.Backend.open(allocator, path, options);
        defer backend.close();
        try std.testing.expect(backend.snapshotOpenStats().wal_replay_records > 0);
        var runtime = try backend.runtimeStore(allocator, .{});
        defer runtime.deinit();
        var manager = try transactions.TxnManager.init(allocator, &runtime);
        defer manager.deinit();
        try std.testing.expectEqual(transactions.TxnStatus.committed, try manager.getTransactionStatus(txn));
        try std.testing.expectEqual(@as(u64, 200), try manager.getCommitVersion(txn));
        const resolved = try manager.getResolvedParticipants(allocator, txn);
        defer transactions.freeParticipantList(allocator, resolved);
        try std.testing.expectEqual(@as(usize, 2), resolved.len);
        const unresolved = try manager.getUnresolvedParticipants(allocator, txn);
        defer transactions.freeParticipantList(allocator, unresolved);
        try std.testing.expectEqual(@as(usize, 0), unresolved.len);
        _ = try manager.resolveIntentsWithExtraBatch(txn, .committed, 200, .{ .resolved_participant = "local" });
        const repeated = try manager.getResolvedParticipants(allocator, txn);
        defer transactions.freeParticipantList(allocator, repeated);
        try std.testing.expectEqual(@as(usize, 2), repeated.len);
    }
}

test "controlled transaction execution admission local acknowledgement policy preserves retained coordinator response" {
    const allocator = std.testing.allocator;
    for ([_]bool{ false, true }) |coordinator| for ([_]bool{ false, true }) |retained| for ([_]transactions.TxnStatus{ .committed, .aborted }) |status| {
        var backend = sources.storage_mem_backend.Backend.init(allocator, .{});
        defer backend.close();
        var runtime = try backend.runtimeStore(allocator, .{});
        defer runtime.deinit();
        var manager = try transactions.TxnManager.init(allocator, &runtime);
        defer manager.deinit();
        const txn: transactions.TxnId = @splat(95);
        try manager.initTransactionWithParticipantsCreatedAtRoleAndRetention(txn, 100, 90, &.{"local"}, coordinator, retained);
        _ = try manager.resolveIntentsWithExtraBatch(txn, status, 200, .{ .resolved_participant = "local", .defer_retained_coordinator_acknowledgement = true });
        const defer_ack = coordinator and retained and status == .committed;
        const resolved = try manager.getResolvedParticipants(allocator, txn);
        defer transactions.freeParticipantList(allocator, resolved);
        try std.testing.expectEqual(@as(usize, if (defer_ack) 0 else 1), resolved.len);
        _ = try manager.resolveIntentsWithExtraBatch(txn, status, 200, .{ .resolved_participant = "local", .defer_retained_coordinator_acknowledgement = true });
        const repeated = try manager.getResolvedParticipants(allocator, txn);
        defer transactions.freeParticipantList(allocator, repeated);
        try std.testing.expectEqual(resolved.len, repeated.len);
        try manager.markParticipantResolved(txn, "local");
        const acknowledged = try manager.getResolvedParticipants(allocator, txn);
        defer transactions.freeParticipantList(allocator, acknowledged);
        try std.testing.expectEqual(@as(usize, 1), acknowledged.len);
    };
}

test "controlled transaction persisted coordinator deferral survives WAL replay" {
    const lsm = sources.storage_lsm_backend;
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const path = try std.fmt.allocPrint(allocator, "/tmp/antfly-coordinator-deferral-wal-{d}", .{@import("antfly_platform").time.monotonicNs()});
    defer allocator.free(path);
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    const txn: transactions.TxnId = @splat(96);
    const options: lsm.Options = .{ .backend = .{ .durability = .full }, .flush_threshold_bytes = 64 * 1024 * 1024 };
    {
        var backend = try lsm.Backend.open(allocator, path, options);
        var abandon = false;
        defer if (abandon) backend.abandonAfterCrash() else backend.close();
        var runtime = try backend.runtimeStore(allocator, .{});
        defer runtime.deinit();
        var manager = try transactions.TxnManager.init(allocator, &runtime);
        defer manager.deinit();
        try manager.initTransactionWithParticipantsCreatedAtRoleAndRetention(txn, 100, 90, &.{ "local", "remote" }, true, true);
        try manager.markParticipantsResolvedExtraBatch(txn, &.{"remote"}, .{});
        try manager.writeIntents(txn, &.{.{ .key = "document", .value = "{}" }}, &.{});
        _ = try manager.resolveIntentsWithExtraBatch(txn, .committed, 200, .{ .resolved_participant = "local", .defer_retained_coordinator_acknowledgement = true });
        try std.testing.expectEqual(@as(usize, 0), backend.runs.count());
        abandon = true;
    }
    {
        var backend = try lsm.Backend.open(allocator, path, options);
        defer backend.close();
        try std.testing.expect(backend.snapshotOpenStats().wal_replay_records > 0);
        var runtime = try backend.runtimeStore(allocator, .{});
        defer runtime.deinit();
        var manager = try transactions.TxnManager.init(allocator, &runtime);
        defer manager.deinit();
        try std.testing.expectEqual(transactions.TxnStatus.committed, try manager.getTransactionStatus(txn));
        const pending = try manager.getUnresolvedParticipants(allocator, txn);
        defer transactions.freeParticipantList(allocator, pending);
        try std.testing.expectEqual(@as(usize, 1), pending.len);
        try std.testing.expectEqualStrings("local", pending[0]);
        try manager.markParticipantResolved(txn, "local");
        const complete = try manager.getUnresolvedParticipants(allocator, txn);
        defer transactions.freeParticipantList(allocator, complete);
        try std.testing.expectEqual(@as(usize, 0), complete.len);
    }
}
