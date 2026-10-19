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

const read = @import("../common/read_admission.zig");
pub const Controls = @import("../storage/db/apply_rw_lock.zig").ApplyRwLock.Acquisition;

const native = @import("antfly_runtime_abi").native_abi;
fn typeContract(comptime T: type) native.TypeContract {
    @setEvalBranchQuota(10_000);
    return native.TypeContract.of(T);
}

const Barrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;

pub const RootContext = struct {
    root: ?*Barrier.RootSharedLease = null,
    retirement: ?*?Barrier.SharedLease = null,
    /// Used only after native commit retires all service holders.
    committed_visibility: ?@import("../common/execution_wait.zig").Boundary = null,
    pub fn validate(self: @This()) !void {
        if (self.retirement) |slot| {
            const root = self.root orelse return error.InvalidArgument;
            if (slot.*) |lease| if (lease.root != root or lease.barrier != root.barrier) return error.InvalidArgument;
        }
    }
};

/// Process-local synchronous batch admission. No pointer is serialized or
/// retained past the owner call. Contracts cover the pointed-to lease layouts.
pub const Payload = struct {
    original: read.Port,
    roots: RootContext = .{},
    root_contract: native.TypeContract = typeContract(Barrier.RootSharedLease),
    lease_contract: native.TypeContract = typeContract(Barrier.SharedLease),
    wait_contract: native.TypeContract = typeContract(@import("../common/execution_wait.zig").Boundary),
};
pub const Port = extern struct {
    contract: native.TypeContract = typeContract(Payload),
    payload: *const anyopaque,
    pub fn init(payload: *const Payload) @This() {
        return .{ .payload = payload };
    }
};

/// Stable synchronous adapter. The caller retains its mutation/service holders
/// and CPU grant; this borrow never creates execution or completion authority.
pub const Borrow = struct {
    original: read.Port = undefined,
    roots: RootContext = .{},
    pub fn bind(self: *@This(), raw: *const anyopaque) !Controls {
        if (@intFromPtr(raw) == 0 or @intFromPtr(raw) % @alignOf(read.Port) != 0) return error.InvalidArgument;
        const header: *const read.Port = @ptrCast(@alignCast(raw));
        self.roots = .{};
        if (header.contract.matches(typeContract(Payload))) {
            if (@intFromPtr(header.payload) % @alignOf(Payload) != 0) return error.InvalidArgument;
            const payload: *const Payload = @ptrCast(@alignCast(header.payload));
            try payload.original.checkpoint();
            if (!payload.root_contract.matches(typeContract(Barrier.RootSharedLease)) or
                !payload.lease_contract.matches(typeContract(Barrier.SharedLease)) or
                !payload.wait_contract.matches(typeContract(@import("../common/execution_wait.zig").Boundary))) return error.UnsupportedVersion;
            try payload.roots.validate();
            self.original = payload.original;
            self.roots = payload.roots;
        } else {
            try header.validate();
            try header.checkpoint();
            self.original = header.*;
        }
        return .{ .lifetime = .{ .context = self, .check_fn = check }, .execution_wait = null };
    }
    fn check(raw: *const anyopaque) !void {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        try self.original.checkpoint();
    }
};

test "controlled owner batch admission borrows original checks and retains holder CPU policy" {
    const std = @import("std");
    const State = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn checkpoint(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks >= self.fail_at) return self.cause;
        }
        fn unexpectedWait(_: *anyopaque) !void {
            return error.UnexpectedCpuLoan;
        }
    };
    var state: State = .{};
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.checkpoint }, .execution_wait = .{ .ptr = &state, .begin = State.unexpectedWait, .end = State.unexpectedWait } };
    const original = read.Port.init(&payload);
    var borrow: Borrow = .{};
    const controls = try borrow.bind(&original);
    try std.testing.expect(controls.execution_wait == null);
    try controls.lifetime.check();
    try std.testing.expectEqual(@as(usize, 2), state.checks);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        state = .{ .fail_at = 1, .cause = cause };
        try std.testing.expectError(cause, borrow.bind(&original));
        state = .{ .fail_at = 2, .cause = cause };
        const admitted = try borrow.bind(&original);
        try std.testing.expectError(cause, admitted.lifetime.check());
    }
}

test "controlled owner batch admission explicit root retires nested and enclosing readers under original ownership" {
    const std = @import("std");
    const Check = struct {
        fn checkpoint(_: *anyopaque) !void {}
        pub fn checkpointRoot(_: *@This()) !void {}
    };
    var marker: u8 = 0;
    const original_payload: read.Payload = .{ .ptr = &marker, .vtable = &.{ .checkpoint = Check.checkpoint } };
    var barrier: Barrier = .{};
    var root = Barrier.RootSharedLease.initRetired(&barrier);
    defer root.release();
    const RootCheck = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn executionBoundary(_: *@This()) ?@import("../common/execution_wait.zig").Boundary {
            return null;
        }
    };
    var check: RootCheck = .{};
    var outer: ?Barrier.SharedLease = try root.borrowPhaseControlled(std.testing.io, &check);
    defer if (outer) |*lease| lease.release();
    const payload: Payload = .{ .original = read.Port.init(&original_payload), .roots = .{ .root = &root, .retirement = &outer } };
    const port = Port.init(&payload);
    var borrow: Borrow = .{};
    const controls = try borrow.bind(&port);
    try std.testing.expect(controls.execution_wait == null);
    try std.testing.expect(borrow.roots.root == &root and borrow.roots.retirement == &outer);
    var inner: ?Barrier.SharedLease = try root.borrowControlled(&check);
    defer if (inner) |*lease| lease.release();
    try std.testing.expectEqual(@as(usize, 2), root.borrowers.load(.acquire));
    const retirement: @import("../storage/db/mutation_admission.zig").MutationRetirement = .{ .current = &inner, .enclosing = borrow.roots.retirement };
    retirement.retire();
    retirement.retire();
    try std.testing.expect(inner == null and outer == null);
    try std.testing.expect(!root.held and root.borrowers.load(.acquire) == 0);
    var capture = barrier.tryAcquireExclusive() orelse return error.TestUnexpectedResult;
    capture.release();
    try controls.lifetime.check();
}

test "controlled owner batch admission root layout and identity reject without retiring another holder" {
    const std = @import("std");
    const Check = struct {
        cause: ?anyerror = null,
        fn checkpoint(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.cause) |cause| return cause;
        }
        pub fn checkpointRoot(_: *@This()) !void {}
    };
    var check: Check = .{};
    const original: read.Payload = .{ .ptr = &check, .vtable = &.{ .checkpoint = Check.checkpoint } };
    var barrier: Barrier = .{};
    var root = Barrier.RootSharedLease.initRetired(&barrier);
    defer root.release();
    var unrelated: ?Barrier.SharedLease = barrier.acquireShared();
    defer if (unrelated) |*lease| lease.release();
    var payload: Payload = .{ .original = read.Port.init(&original), .roots = .{ .root = &root, .retirement = &unrelated } };
    const port = Port.init(&payload);
    var borrow: Borrow = .{};
    try std.testing.expectError(error.InvalidArgument, borrow.bind(&port));
    try std.testing.expect(unrelated != null and !root.held);
    payload.roots = .{};
    payload.wait_contract.version += 1;
    try std.testing.expectError(error.UnsupportedVersion, borrow.bind(&port));
    payload.wait_contract = typeContract(@import("../common/execution_wait.zig").Boundary);
    payload.root_contract.version += 1;
    try std.testing.expectError(error.UnsupportedVersion, borrow.bind(&port));
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        check.cause = cause;
        try std.testing.expectError(cause, borrow.bind(&port));
        try std.testing.expect(unrelated != null and !root.held);
    }
    try std.testing.expectEqual(@as(usize, 1), barrier.reader_count);
}
