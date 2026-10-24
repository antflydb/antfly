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

const runtime = @import("antfly_runtime_abi");
const Controls = @import("antfly_local_sources").storage_db_apply_rw_lock.ApplyRwLock.Acquisition;

/// Synchronous pre-decision borrow. Service holders still require their CPU
/// grant; this adapter supplies original lifetime without lending that grant.
pub const Borrow = struct {
    original: runtime.runnable_abi.Borrowed = undefined,
    pub fn bind(self: *@This(), original: runtime.runnable_abi.Borrowed) !Controls {
        try original.checkLifetime();
        self.original = original;
        return .{ .lifetime = .{ .context = self, .check_fn = check }, .execution_wait = null };
    }
    fn check(raw: *const anyopaque) !void {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        try self.original.checkLifetime();
    }
};

test "controlled transaction execution admission retains original lifetime and holder CPU" {
    const std = @import("std");
    const State = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            return if (self.checks >= self.fail_at) runtime.error_abi.statusFromError(self.cause) else .ok;
        }
        fn cpu(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
    };
    var state: State = .{};
    const original: runtime.runnable_abi.Borrowed = .{ .context = &state, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.write), .check_lifetime = State.lifetime, .checkpoint = State.cpu, .park = State.phase, .enter = State.phase };
    var borrow: Borrow = .{};
    const controls = try borrow.bind(original);
    try std.testing.expect(controls.execution_wait == null);
    try controls.lifetime.check();
    try std.testing.expectEqual(@as(usize, 2), state.checks);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        state = .{ .fail_at = 1, .cause = cause };
        try std.testing.expectError(cause, borrow.bind(original));
        state = .{ .fail_at = 2, .cause = cause };
        const admitted = try borrow.bind(original);
        try std.testing.expectError(cause, admitted.lifetime.check());
    }
}
