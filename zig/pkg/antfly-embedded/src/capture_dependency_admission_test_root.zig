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

pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
test "capture dependency admission Lite locks retire before original CPU wait" {
    try @import("storage/lite/docstore.zig").qualifySnapshotCaptureDependencies();
}
test "capture dependency admission publication rejects coarse waits and preserves original scope" {
    try @import("storage/docstore.zig").qualifyPortablePublicationAdmission();
}

test "capture dependency admission retained readers permit legacy and coarse publishers to yield progress" {
    const std = @import("std");
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    const docstore = @import("storage/docstore.zig");
    const erased = @import("storage/backend_erased.zig");
    const read_admission = @import("common/read_admission.zig");
    const scheduling = @import("common/runnable_scheduler.zig");
    const Gate = @import("storage/db/row_policy_gate.zig").Gate;
    const Apply = @import("storage/db/apply_rw_lock.zig").ApplyRwLock;
    const Controls = Apply.Acquisition;
    const Kind = enum { snapshot, probe, current };
    const Mode = enum { readers, apply, readers_coarse, apply_coarse, canceled, deadline, changed };
    const Owner = struct {
        core: struct {
            store: *docstore.DocStore,
            apply_mutex: Apply = .{},
            identity_namespace: @import("storage/db/doc_identity_namespace.zig").Namespace = .{},
            pub fn byteRange(_: *@This()) @import("storage/byte_range.zig").ByteRange {
                return .{ .start = &.{}, .end = &.{} };
            }
            pub fn unlockApply(self: *@This()) void {
                self.apply_mutex.unlockExclusive();
            }
        },
        local_execution: struct { row_policy_gate: Gate = .{} } = .{},
        backend_runtime: struct {
            pub fn io(_: *@This()) ?std.Io {
                return std.testing.io;
            }
            pub fn filesystemIo(_: *@This()) ?std.Io {
                return std.testing.io;
            }
        } = .{},
        pub fn enforcePortableRuntimeGate(_: *@This()) !void {}
        pub fn lockApplyForPortableRuntime(self: *@This()) !void {
            self.core.apply_mutex.lockExclusive();
        }
        fn pristine(_: *@This(), _: std.mem.Allocator, _: ?Controls) !bool {
            return true;
        }
    };
    const Fixture = struct {
        const Self = @This();
        owner: *Owner,
        original: erased.Store,
        scheduler: *scheduling.Scheduler,
        kind: Kind,
        mode: Mode,
        memory: ?*@import("storage/mem_backend.zig").Backend = null,
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        done: std.Io.Event = .unset,
        result: ?anyerror = null,
        stop: std.atomic.Value(bool) = .init(false),
        begins: usize = 0,
        ends: usize = 0,
        fn awaitCapture(self: *Self, port: read_admission.Port) !void {
            try std.testing.expectEqual(@as(usize, 1), self.owner.core.store.portable_import_reader_state.load(.acquire));
            const apply = self.mode == .apply or self.mode == .apply_coarse;
            if (apply) self.owner.core.apply_mutex.lockShared();
            defer if (apply) self.owner.core.apply_mutex.unlockShared();
            const boundary = (try port.executionWait()) orelse return error.ExecutionWaitUnsupported;
            try boundary.beginWait();
            self.entered.set(std.testing.io);
            self.release.waitUncancelable(std.testing.io);
            try boundary.endWait();
        }
        fn snapshot(_: std.mem.Allocator, raw: *anyopaque, cache: @import("storage/backend_types.zig").Namespace.BlockCacheAdmission, port: read_admission.Port) !erased.ReadTxn {
            const self: *Self = @ptrCast(@alignCast(raw));
            try self.awaitCapture(port);
            return self.original.beginReadWithAdmission(cache, port);
        }
        fn probe(_: std.mem.Allocator, raw: *anyopaque, cache: @import("storage/backend_types.zig").Namespace.BlockCacheAdmission, port: read_admission.Port) !erased.ProbeTxn {
            const self: *Self = @ptrCast(@alignCast(raw));
            try self.awaitCapture(port);
            return self.original.beginProbeWithAdmission(cache, port);
        }
        fn current(_: std.mem.Allocator, raw: *anyopaque, port: read_admission.Port) !erased.CurrentScanTxn {
            const self: *Self = @ptrCast(@alignCast(raw));
            try self.awaitCapture(port);
            return self.original.beginCurrentScanWithAdmission(port);
        }
        const Scope = struct {
            f: *Self,
            execution: scheduling.Execution = .{},
            reject: ?anyerror = null,
            pub fn checkLifetime(self: *@This()) !void {
                if (self.reject) |cause| return cause;
                if (self.f.stop.load(.acquire)) return error.Canceled;
                try self.execution.checkLifetime();
            }
            pub fn checkpoint(self: *@This()) !void {
                try self.checkLifetime();
                if (self.f.memory) |memory| {
                    // A yielding checkpoint is never allowed under the
                    // captured backend's coordination mutex.
                    try std.testing.expect(memory.mutex.tryLock());
                    memory.mutex.unlock();
                }
            }
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try self.checkLifetime();
            }
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.f.begins += 1;
                try self.execution.park(.resource_blocked);
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.f.ends += 1;
                self.reject = switch (self.f.mode) {
                    .canceled => error.Canceled,
                    .deadline => error.DeadlineExceeded,
                    .changed => error.CatalogGenerationChanged,
                    else => null,
                };
                try self.execution.enterWithCheck(.query, .{ .context = self, .check = check });
            }
            pub fn executionBoundary(self: *@This()) ?@import("common/execution_wait.zig").Boundary {
                return .{ .ptr = self, .begin = begin, .end = end };
            }
            pub fn waitForProgress(self: *@This()) !void {
                try begin(self);
                const result = std.testing.io.sleep(.fromMilliseconds(1), .awake);
                try end(self);
                try result;
            }
            fn run(self: *@This()) void {
                self.execute() catch |cause| {
                    self.f.result = cause;
                };
                self.f.done.set(std.testing.io);
            }
            fn execute(self: *@This()) !void {
                const now: u64 = @intCast(std.Io.Clock.now(.awake, std.testing.io).nanoseconds);
                try self.execution.start(self.f.scheduler, .{ .io = std.testing.io, .allow_wait = true, .deadline_ns = now + 10 * std.time.ns_per_s, .retained_bytes = 17 }, .query);
                defer self.execution.finish();
                var adapter: read_admission.Adapter(*@This()) = .{ .budget = self };
                const payload = adapter.payload();
                const port: read_admission.Port = .init(&payload);
                var txn = try switch (self.f.kind) {
                    .snapshot => self.f.owner.core.store.beginReadTxnWithAdmission(.retain, port),
                    .probe => self.f.owner.core.store.beginProbeTxnWithAdmission(.retain, port),
                    .current => self.f.owner.core.store.beginCurrentScanTxnWithAdmission(port),
                };
                defer txn.abort();
                try std.testing.expectEqual(@as(usize, 1), self.f.owner.core.store.portable_import_reader_state.load(.acquire));
                if (self.f.kind == .current) {
                    var cursor = try txn.openCursor();
                    defer cursor.close();
                    try std.testing.expect(try cursor.first() == null);
                } else try std.testing.expectError(error.NotFound, txn.get("missing"));
            }
        };
    };
    const a = std.testing.allocator;
    const io = std.testing.io;
    const Backend = enum { memory, lsm, lite };
    const timeout: std.Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
    for (std.enums.values(Backend)) |backend_kind| {
        var directory = try @import("common/test_directory.zig").TestDirectory.init("capture-native.aflite");
        defer directory.cleanup();
        var memory = @import("storage/mem_backend.zig").Backend.init(a, .{});
        defer memory.close();
        var lsm = @import("storage/lsm_backend.zig").Backend.init(a, .{});
        defer lsm.close();
        var lite = try @import("storage/lite/docstore.zig").Store.createWithOptions(a, directory.path(), .{ .io = io, .no_sync = true, .reclamation = .{ .enabled = false } });
        defer lite.close();
        for (std.enums.values(Kind)) |kind| {
            if (backend_kind == .lite and kind != .snapshot) continue;
            for (std.enums.values(Mode)) |mode| {
                const runtime = switch (backend_kind) {
                    .memory => try memory.runtimeStore(a, .{}),
                    .lsm => try lsm.runtimeStore(a, .{}),
                    .lite => try lite.runtimeStore(a),
                };
                var store = try docstore.DocStore.openRuntime(a, runtime);
                defer store.close();
                var owner: Owner = .{ .core = .{ .store = &store } };
                var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
                defer scheduler.close();
                var f: Fixture = .{ .owner = &owner, .original = store.runtime_store, .scheduler = &scheduler, .kind = kind, .mode = mode, .memory = if (backend_kind == .memory) &memory else null };
                var vtable = f.original.vtable.*;
                vtable.begin_read_with_admission = Fixture.snapshot;
                vtable.begin_probe_with_admission = Fixture.probe;
                vtable.begin_current_scan_with_admission = Fixture.current;
                store.runtime_store.ptr = &f;
                store.runtime_store.vtable = &vtable;
                defer store.runtime_store = f.original;
                var reader: Fixture.Scope = .{ .f = &f };
                const thread = try std.Thread.spawn(.{}, Fixture.Scope.run, .{&reader});
                var joined = false;
                defer if (!joined) {
                    f.stop.store(true, .release);
                    f.release.set(io);
                    thread.join();
                };
                f.entered.waitTimeout(io, timeout) catch |cause| {
                    f.stop.store(true, .release);
                    f.release.set(io);
                    thread.join();
                    joined = true;
                    std.debug.print("capture entry failure result={any} begins={d} ends={d}\n", .{ f.result, f.begins, f.ends });
                    return f.result orelse cause;
                };
                var publisher: scheduling.Execution = .{};
                try publisher.start(&scheduler, .{ .io = io, .allow_wait = false, .retained_bytes = 23 }, .write);
                defer publisher.finish();
                try std.testing.expectEqual(@as(u64, 40), scheduler.stats().retained_bytes);
                var health: scheduling.Execution = .{};
                try health.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
                health.finish();
                var policy = try owner.local_execution.row_policy_gate.enterRaw();
                defer policy.release();
                const Life = struct {
                    fn check(raw: *const anyopaque) !void {
                        const e: *const scheduling.Execution = @ptrCast(@alignCast(raw));
                        try @constCast(e).checkLifetime();
                    }
                };
                const controls: ?Controls = if (mode == .readers_coarse or mode == .apply_coarse) .{ .lifetime = .{ .context = &publisher, .check_fn = Life.check } } else null;
                try std.testing.expectError(error.Backpressured, @import("storage/db/portable_publication_admission.zig").acquire(&owner, a, &policy, controls, Owner.pristine));
                try std.testing.expect(!store.portableImportPublicationInProgress());
                try std.testing.expectEqual(@as(usize, 1), store.portable_import_reader_state.load(.acquire));
                try std.testing.expectError(error.Backpressured, store.beginPortableImportPublication(io));
                try std.testing.expect(!store.portableImportPublicationInProgress());
                policy.release();
                publisher.finish();
                f.release.set(io);
                f.done.waitTimeout(io, timeout) catch |cause| {
                    f.stop.store(true, .release);
                    f.release.set(io);
                    thread.join();
                    joined = true;
                    std.debug.print("capture exit failure result={any} begins={d} ends={d}\n", .{ f.result, f.begins, f.ends });
                    return f.result orelse cause;
                };
                thread.join();
                joined = true;
                const expected: ?anyerror = switch (mode) {
                    .canceled => error.Canceled,
                    .deadline => error.DeadlineExceeded,
                    .changed => error.CatalogGenerationChanged,
                    else => null,
                };
                try std.testing.expectEqual(expected, f.result);
                try std.testing.expectEqual(@as(usize, 1), f.begins);
                try std.testing.expectEqual(@as(usize, 1), f.ends);
                try std.testing.expectEqual(@as(usize, 0), store.portable_import_reader_state.load(.acquire));
                try std.testing.expect(owner.core.apply_mutex.tryLockExclusive());
                owner.core.unlockApply();
                try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
                try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
            }
        }
    }
}

test "capture dependency admission original ports and acquired shared locks preserve wait authority" {
    const std = @import("std");
    const admission = @import("common/read_admission.zig");
    const Fixture = struct {
        held: bool = false,
        late_error: bool = false,
        checks: usize = 0,
        lives: usize = 0,
        pub fn tryLockShared(self: *@This()) bool {
            if (self.held) return false;
            self.held = true;
            return true;
        }
        pub fn unlockShared(self: *@This()) void {
            self.held = false;
        }
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            try std.testing.expect(!self.held);
        }
        pub fn checkLifetime(self: *@This()) !void {
            self.lives += 1;
            if (self.held and self.late_error) return error.CatalogGenerationChanged;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.UnexpectedCaptureWait;
        }
        fn boundary(_: *anyopaque) !void {
            return error.UnexpectedCaptureWait;
        }
        pub fn executionBoundary(self: *@This()) ?@import("common/execution_wait.zig").Boundary {
            return .{ .ptr = self, .begin = boundary, .end = boundary };
        }
    };
    for ([_]bool{ false, true }) |late_error| {
        var f: Fixture = .{ .late_error = late_error };
        var adapter: admission.Adapter(*Fixture) = .{ .budget = &f };
        const payload = adapter.payload();
        var original: admission.Port = .init(&payload);
        var fallback_adapter: admission.Adapter(admission.Port) = .{ .budget = original };
        const fallback = fallback_adapter.payload();
        const direct = admission.portForBudget(original, &fallback);
        const pointer = admission.portForBudget(&original, &fallback);
        try std.testing.expect(direct.payload == original.payload and pointer.payload == original.payload);
        try std.testing.expect(try direct.hasExecutionWait());
        const result = admission.lockShared(&f, direct);
        if (late_error) {
            try std.testing.expectError(error.CatalogGenerationChanged, result);
            try std.testing.expect(!f.held);
        } else {
            try result;
            try std.testing.expect(f.held);
            f.unlockShared();
        }
        try std.testing.expectEqual(@as(usize, 1), f.checks);
        try std.testing.expectEqual(@as(usize, 1), f.lives);
        original.contract.size +%= 1;
        const invalid = admission.portForBudget(original, &fallback);
        try std.testing.expectError(error.UnsupportedVersion, invalid.hasExecutionWait());
    }
}

test "capture dependency admission incomplete lending budgets reject before provider ownership" {
    const std = @import("std");
    var backend = @import("storage/mem_backend.zig").Backend.init(std.testing.allocator, .{});
    defer backend.close();
    var store = try @import("storage/docstore.zig").DocStore.openRuntime(std.testing.allocator, try backend.runtimeStore(std.testing.allocator, .{}));
    defer store.close();
    const Budget = struct {
        checks: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.checks += 1;
            return error.UnexpectedIncompleteCaptureCheckpoint;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.UnexpectedIncompleteCaptureWait;
        }
        fn boundary(_: *anyopaque) !void {
            return error.UnexpectedIncompleteCaptureWait;
        }
        pub fn executionBoundary(self: *@This()) ?@import("common/execution_wait.zig").Boundary {
            return .{ .ptr = self, .begin = boundary, .end = boundary };
        }
    };
    var budget: Budget = .{};
    try std.testing.expectError(error.ExecutionWaitUnsupported, store.beginReadTxnWithBudget(.retain, &budget));
    try std.testing.expectEqual(@as(usize, 0), budget.checks);
    try std.testing.expectEqual(@as(usize, 0), store.portable_import_reader_state.load(.acquire));
    try std.testing.expect(store.payload_policy_mutex.tryLock());
    store.payload_policy_mutex.unlock(std.testing.io);
    try std.testing.expect(backend.mutex.tryLock());
    backend.mutex.unlock();
}

test "capture dependency admission retained authority preserves scope failures and forbids handoff" {
    const std = @import("std");
    const admission = @import("common/read_admission.zig");
    const State = struct {
        checks: usize = 0,
        cause: ?anyerror = null,
        fn cpu(_: *anyopaque) !void {
            return error.UnexpectedRetainedCpuCheckpoint;
        }
        fn life(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.cause) |cause| {
                self.cause = null; // first rejection must remain authoritative
                return cause;
            }
        }
        fn time(_: *anyopaque) !?u64 {
            return 1234567;
        }
        fn handoff(_: *anyopaque) !void {
            return error.UnexpectedRetainedHandoff;
        }
    };
    var state: State = .{};
    const payload: admission.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.cpu, .check_lifetime = State.life, .deadline_ns = State.time, .wait_for_progress = State.handoff }, .execution_wait = .{ .ptr = &state, .begin = State.handoff, .end = State.handoff } };
    const original: admission.Port = .init(&payload);
    const physical = (try original.executionWaitAfterLifetime()).?;
    try std.testing.expectEqual(@intFromPtr(&state), @intFromPtr(physical.ptr));
    var retained: admission.RetainedDependency = .{};
    try std.testing.expect(retained.bind(null) == null);
    const control = retained.bind(.init(&payload)).?;
    try control.checkpoint();
    try std.testing.expectEqual(@as(?u64, 1234567), try control.deadlineNs());
    try std.testing.expect(try control.executionWait() == null);
    try std.testing.expect(try control.executionWaitAfterLifetime() == null);
    try std.testing.expectError(error.Backpressured, control.waitForProgress());
    try control.checkpoint(); // retryable availability must not poison authority
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.RowPolicyCatalogChanged, error.NotFound }) |cause| {
        const scope = retained.bind(.init(&payload)).?;
        state.cause = cause;
        try std.testing.expectError(cause, scope.checkpoint());
        const checks = state.checks;
        try std.testing.expectError(cause, scope.waitForProgress());
        try std.testing.expectError(cause, scope.checkLifetime());
        try std.testing.expectError(cause, scope.deadlineNs());
        try std.testing.expectEqual(checks, state.checks);
    }
    var invalid: admission.Port = .init(&payload);
    invalid.contract.size += 1;
    try std.testing.expectError(error.UnsupportedVersion, invalid.executionWaitAfterLifetime());
    try std.testing.expectError(error.UnsupportedVersion, retained.bind(invalid).?.checkpoint());
}

test "capture dependency admission intent probe retires busy captures before retry and preserves original errors" {
    const std = @import("std");
    const a = std.testing.allocator;
    const admission = @import("common/read_admission.zig");
    const Store = @import("storage/docstore.zig").DocStore;
    const TxnManager = @import("storage/transactions.zig").TxnManager;
    const Fence = @import("storage/db/snapshot_admission.zig").SnapshotAdmission;
    const State = struct {
        checks: usize = 0,
        fail_at: usize = 0,
        cause: anyerror = error.Canceled,
        fn cpu(_: *anyopaque) !void {
            return error.UnexpectedIntentCpuCheckpoint;
        }
        fn life(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
        fn handoff(_: *anyopaque) !void {
            return error.UnexpectedIntentHandoff;
        }
    };
    var directory = try @import("common/test_directory.zig").TestDirectory.init("intent-fence-busy.aflite");
    defer directory.cleanup();
    var backend = try @import("storage/lite/docstore.zig").Store.createWithOptions(a, directory.path(), .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer backend.close();
    var store = try Store.openRuntime(a, try backend.runtimeStore(a));
    defer store.close();
    var manager = try TxnManager.init(a, &store);
    defer manager.deinit();
    var primary: Fence = .{};
    var replay: Fence = .{};
    var state: State = .{};
    const payload: admission.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.cpu, .check_lifetime = State.life, .wait_for_progress = State.handoff }, .execution_wait = .{ .ptr = &state, .begin = State.handoff, .end = State.handoff } };
    const control: admission.Port = .init(&payload);
    {
        var first = primary.tryAcquireCapture() orelse return error.TestUnexpectedResult;
        defer first.release();
        var second = replay.tryAcquireCapture() orelse return error.TestUnexpectedResult;
        defer second.release();
        backend.read_mutex.lockUncancelable(std.testing.io);
        defer backend.read_mutex.unlock(std.testing.io);
        try std.testing.expectError(error.Backpressured, manager.hasUnresolvedWriteIntentsWithAdmission(control));
        try std.testing.expectEqual(@as(u32, 0), store.portable_import_reader_state.load(.acquire));
    }
    // A publisher can acquire both gates after complete partial-capture
    // retirement. The original scope is reused for the next capture attempt.
    try std.testing.expect(primary.lock.tryLockShared());
    primary.lock.unlockShared();
    try std.testing.expect(replay.lock.tryLockShared());
    replay.lock.unlockShared();
    for ([_]bool{ false, true }) |has_intent| {
        if (has_intent) try store.put("\x00\x00__txn_intents__:pending", "presence-only");
        var first = primary.tryAcquireCapture() orelse return error.TestUnexpectedResult;
        defer first.release();
        var second = replay.tryAcquireCapture() orelse return error.TestUnexpectedResult;
        defer second.release();
        try std.testing.expect(!primary.lock.tryLockShared());
        try std.testing.expect(!replay.lock.tryLockShared());
        state = .{};
        try std.testing.expectEqual(has_intent, try manager.hasUnresolvedWriteIntentsWithAdmission(control));
        state = .{}; // measure the warmed retry path, not cold initialization
        try std.testing.expectEqual(has_intent, try manager.hasUnresolvedWriteIntentsWithAdmission(control));
        const checks = state.checks;
        try std.testing.expect(checks > 0);
        for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.RowPolicyCatalogChanged, error.NotFound }) |cause| {
            for (1..checks + 1) |fail_at| {
                state = .{ .fail_at = fail_at, .cause = cause };
                try std.testing.expectError(cause, manager.hasUnresolvedWriteIntentsWithAdmission(control));
                try std.testing.expectEqual(@as(u32, 0), store.portable_import_reader_state.load(.acquire));
                try std.testing.expectEqual(@as(usize, 0), backend.read_generation.?.kernel_readers);
                try std.testing.expect(backend.read_mutex.tryLock());
                backend.read_mutex.unlock(std.testing.io);
            }
        }
    }
}

test "capture dependency admission fence wrappers forward retained authority and retire rejected results once" {
    const std = @import("std");
    const admission = @import("common/read_admission.zig");
    const fences = @import("storage/statement_read_fence.zig");
    const View = @import("storage/relational_read_view.zig").View;
    const Options = @import("storage/db/types.zig").ScanOptions;
    const Fixture = struct {
        late: bool = false,
        rejected: bool = false,
        released: usize = 0,
        closed: usize = 0,
        fn cpu(_: *anyopaque) !void {
            return error.UnexpectedFenceHandoff;
        }
        fn life(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.rejected) return error.NotFound;
        }
        fn verify(control: admission.Port) !void {
            try control.checkpoint();
            try std.testing.expect(try control.executionWait() == null);
            try std.testing.expectError(error.Backpressured, control.waitForProgress());
        }
        fn validate(_: *anyopaque) !void {}
        fn validateAdmitted(_: *anyopaque, control: admission.Port) !void {
            try verify(control);
        }
        fn open(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: Options) !View {
            return error.UnexpectedLegacyFenceOpen;
        }
        fn openAdmitted(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: Options, control: admission.Port) !View {
            try verify(control);
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.rejected = self.late;
            return .{ .ptr = raw, .vtable = &.{ .next = next, .close = close } };
        }
        fn next(_: *anyopaque, _: std.mem.Allocator, _: u32) !View.Page {
            return error.UnexpectedTestPage;
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closed += 1;
        }
        fn capture(raw: *anyopaque, _: std.mem.Allocator, control: admission.Port) !fences.Snapshot {
            try verify(control);
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.rejected = self.late;
            return .{ .ptr = raw, .vtable = &.{ .open = open, .release = close } };
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.released += 1;
        }
    };
    for ([_]bool{ false, true }) |late| {
        var fixture: Fixture = .{ .late = late };
        const payload: admission.Payload = .{ .ptr = &fixture, .vtable = &.{ .checkpoint = Fixture.cpu, .check_lifetime = Fixture.life, .wait_for_progress = Fixture.cpu }, .execution_wait = .{ .ptr = &fixture, .begin = Fixture.cpu, .end = Fixture.cpu } };
        const control: admission.Port = .init(&payload);
        const fence: fences.Fence = .{ .ptr = &fixture, .vtable = &.{ .validate = Fixture.validate, .validate_with_admission = Fixture.validateAdmitted, .open = Fixture.open, .open_with_admission = Fixture.openAdmitted, .capture_snapshot_with_admission = Fixture.capture, .release = Fixture.release } };
        try fence.validateWithAdmission(control);
        const opened = fence.openWithAdmission(std.testing.allocator, "", "", .{}, control);
        if (late) try std.testing.expectError(error.NotFound, opened) else (try opened).deinit();
        try std.testing.expectEqual(@as(usize, 1), fixture.closed);
        fixture.rejected = false;
        const captured = fence.captureSnapshotWithAdmission(std.testing.allocator, control);
        if (late) try std.testing.expectError(error.NotFound, captured) else (try captured).deinit();
        try std.testing.expectEqual(@as(usize, 2), fixture.closed);
        try std.testing.expectEqual(@as(usize, 0), fixture.released);
        fence.deinit();
        try std.testing.expectEqual(@as(usize, 1), fixture.released);
    }
}
