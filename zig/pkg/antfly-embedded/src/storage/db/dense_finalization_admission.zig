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

const manager_mod = @import("catalog/index_manager.zig");
const sessions_mod = @import("dense_catch_up_session_owner.zig");
const runtime_types = @import("derived/runtime_types.zig");
const admission = @import("../../common/read_admission.zig");

pub const Prepared = struct {
    session: sessions_mod.Session,
    apply_guard: manager_mod.IndexManager.ManagedIndexApplyGuard,
};

/// Borrow original control only through preparation. Nothing is consumed until
/// catalog/apply and session-registry admission have all succeeded. A failed
/// attempt owns no guards and leaves the exact token and replay lease intact.
pub fn prepare(manager: *manager_mod.IndexManager, sessions: *sessions_mod.Owner, index_ref: manager_mod.ManagedIndexRef, token: runtime_types.CatchUpSessionToken, original: admission.Port) !Prepared {
    while (true) {
        var guard = try manager.lockManagedIndexApplyWithAdmission(index_ref, original);
        const maybe_session = sessions.tryTake(index_ref.name, token) catch |cause| {
            guard.unlock();
            return cause;
        };
        if (maybe_session) |session| return .{ .session = session, .apply_guard = guard };
        guard.unlock();
        try @import("../../common/cleanup_wait.zig").waitForRetry(manager.checkpointIo(), original);
    }
}

test "controlled dense finalization preparation preserves session admission across all guard failures" {
    const std = @import("std");
    const Snapshot = @import("snapshot_admission.zig").SnapshotAdmission;
    const Case = enum { success, begin_failure, end_failure, lifetime_in_wait };
    const Blocker = enum { catalog, apply, registry };
    for (std.enums.values(Blocker)) |blocker| {
        for (std.enums.values(Case)) |case| {
            var manager: manager_mod.IndexManager = undefined;
            manager.io = std.testing.io;
            manager.catalog_mutex = .{ .io = std.testing.io };
            manager.text_indexes = .empty;
            manager.dense_indexes = .empty;
            manager.sparse_indexes = .empty;
            manager.graph_indexes = .empty;
            manager.algebraic_indexes = .empty;
            var mutex: std.atomic.Mutex = .unlocked;
            var entries: [1]manager_mod.IndexManager.DenseIndex = undefined;
            entries[0].config.name = "idx";
            entries[0].apply_mutex = &mutex;
            manager.dense_indexes.items = &entries;
            manager.dense_indexes.capacity = 1;
            var sessions: sessions_mod.Owner = .{};
            defer sessions.deinit(std.testing.allocator, undefined);
            var snapshot: Snapshot = .{};
            var mutation: ?Snapshot.MutationLease = snapshot.acquireMutation();
            const token = try sessions.install(std.testing.allocator, "idx", 17, null, &mutation);
            const Control = struct {
                manager: *manager_mod.IndexManager,
                apply: *std.atomic.Mutex,
                sessions: *sessions_mod.Owner,
                blocker: Blocker,
                case: Case,
                held: bool = true,
                begins: usize = 0,
                ends: usize = 0,
                fn release(self: *@This()) void {
                    if (!self.held) return;
                    switch (self.blocker) {
                        .catalog => self.manager.catalog_mutex.unlockExclusive(),
                        .apply => self.apply.unlock(),
                        .registry => self.sessions.mutex.unlock(),
                    }
                    self.held = false;
                }
                fn check(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    if (self.case == .lifetime_in_wait and self.begins != 0) return error.DeadlineExceeded;
                }
                fn deadline(_: *anyopaque) !?u64 {
                    return 1000;
                }
                fn begin(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.begins += 1;
                    if (self.blocker != .catalog) {
                        try std.testing.expect(self.manager.catalog_mutex.tryLockExclusive());
                        self.manager.catalog_mutex.unlockExclusive();
                    }
                    if (self.blocker != .apply) {
                        try std.testing.expect(self.apply.tryLock());
                        self.apply.unlock();
                    }
                    if (self.case == .begin_failure) return error.NotFound;
                    if (self.case != .lifetime_in_wait) self.release();
                }
                fn end(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.ends += 1;
                    self.release();
                    // No attempt may acquire catalog/apply while parked.
                    try std.testing.expectEqual(@as(usize, 0), self.manager.catalog_mutex.state.load(.acquire));
                    try std.testing.expect(self.apply.tryLock());
                    self.apply.unlock();
                    if (self.case == .end_failure) return error.Canceled;
                }
            };
            switch (blocker) {
                .catalog => manager.catalog_mutex.lockExclusive(),
                .apply => try std.testing.expect(mutex.tryLock()),
                .registry => try std.testing.expect(sessions.mutex.tryLock()),
            }
            var control: Control = .{ .manager = &manager, .apply = &mutex, .sessions = &sessions, .blocker = blocker, .case = case };
            defer control.release();
            var payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
            const ref: manager_mod.ManagedIndexRef = .{ .name = "idx", .kind = .dense_vector };
            var prepared = prepare(&manager, &sessions, ref, token, .init(&payload)) catch |cause| retry: {
                if (case == .success) return cause;
                try std.testing.expectEqual(switch (case) {
                    .begin_failure => error.NotFound,
                    .end_failure => error.Canceled,
                    .lifetime_in_wait => error.DeadlineExceeded,
                    .success => unreachable,
                }, cause);
                control.release();
                try std.testing.expectEqual(@as(usize, 1), sessions.sessions.count());
                try std.testing.expect(sessions.sessions.contains(token.value));
                try std.testing.expect(!snapshot.lock.tryLockExclusive());
                try std.testing.expect(manager.catalog_mutex.tryLockExclusive());
                manager.catalog_mutex.unlockExclusive();
                try std.testing.expect(mutex.tryLock());
                mutex.unlock();
                control.case = .success;
                break :retry try prepare(&manager, &sessions, ref, token, .init(&payload));
            };
            try std.testing.expectEqual(@as(usize, 0), sessions.sessions.count());
            try std.testing.expectEqual(@as(u64, 17), prepared.session.index_incarnation);
            try std.testing.expect(!snapshot.lock.tryLockExclusive());
            prepared.apply_guard.unlock();
            std.testing.allocator.free(prepared.session.index_name);
            if (prepared.session.snapshot_replay) |*lease| lease.release();
            try std.testing.expect(snapshot.lock.tryLockExclusive());
            snapshot.lock.unlockExclusive();
            try std.testing.expectEqual(control.begins - @intFromBool(case == .begin_failure), control.ends);
        }
    }
}
