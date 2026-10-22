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

//! Synchronous admission borrow. Never retained by a returned read transaction.
const native = @import("antfly_runtime_abi").native_abi;
const callback = @import("../runtime_callback_abi.zig");
/// Owned retirement metadata is copied only after validating this synchronous
/// borrow. Its parent and registry must outlive every published obligation.
pub const Retirement = @import("read_retirement.zig");
const HttpExecutor = @import("http/http_common.zig").RequestExecutor;
pub const VTable = struct {
    checkpoint: *const fn (*anyopaque) anyerror!void,
    wait_for_progress: ?*const fn (*anyopaque) anyerror!void = null,
    check_lifetime: ?*const fn (*anyopaque) anyerror!void = null,
    deadline_ns: ?*const fn (*anyopaque) anyerror!?u64 = null,
    retirement_binding: ?*const fn (*anyopaque) anyerror!?Retirement.Binding = null,
    remote_executor: ?*const fn (*anyopaque) anyerror!?HttpExecutor = null,
};
pub const Boundary = callback.Boundary(VTable);
pub const Payload = struct {
    vtable_contract: native.TypeContract = .of(VTable),
    ptr: *anyopaque,
    vtable: *const VTable,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    execution_wait: ?@import("execution_wait.zig").Boundary = null,
};
pub const Port = extern struct {
    contract: native.TypeContract = .of(Payload),
    payload: *const anyopaque,

    pub fn init(payload: *const Payload) Port {
        return .{ .payload = payload };
    }
    pub fn validate(self: Port) !void {
        if (!self.contract.matches(.of(Payload))) return error.UnsupportedVersion;
        if (@intFromPtr(self.payload) % @alignOf(Payload) != 0) return error.InvalidArgument;
    }
    pub fn checkpoint(self: Port) !void {
        try self.validate();
        const payload: *const Payload = @ptrCast(@alignCast(self.payload));
        if (!payload.vtable_contract.matches(.of(VTable))) return error.UnsupportedVersion;
        try Boundary.call("checkpoint", payload.dispatch, payload.vtable.checkpoint, .{payload.ptr});
    }
    /// Safe while CPU is parked. Legacy ports may use their checkpoint only
    /// when they cannot release CPU; reject an incomplete suspended borrow.
    pub fn checkLifetime(self: Port) !void {
        try self.validate();
        const payload: *const Payload = @ptrCast(@alignCast(self.payload));
        if (!payload.vtable_contract.matches(.of(VTable))) return error.UnsupportedVersion;
        if (payload.vtable.check_lifetime) |check|
            return Boundary.call("check_lifetime", payload.dispatch, check, .{payload.ptr});
        if (payload.execution_wait != null) return error.ExecutionWaitUnsupported;
        return self.checkpoint();
    }
    pub fn deadlineNs(self: Port) !?u64 {
        try self.checkLifetime();
        const payload: *const Payload = @ptrCast(@alignCast(self.payload));
        const deadline = payload.vtable.deadline_ns orelse return null;
        return Boundary.call("deadline_ns", payload.dispatch, deadline, .{payload.ptr});
    }
    /// Borrow only long enough to copy a stable owned parent capability. Never
    /// return or retain this Port as a cleanup owner. Scope/contract failures
    /// remain errors; only an explicitly absent capability returns null.
    pub fn retirementBinding(self: Port) !?Retirement.Binding {
        try self.checkLifetime();
        const payload: *const Payload = @ptrCast(@alignCast(self.payload));
        const get = payload.vtable.retirement_binding orelse return null;
        return Boundary.call("retirement_binding", payload.dispatch, get, .{payload.ptr});
    }
    /// Synchronous query dispatch capability. A returned resource must copy
    /// its stable node descriptor and own the full parent before this borrow ends.
    pub fn remoteExecutor(self: Port) !?HttpExecutor {
        try self.checkLifetime();
        const payload: *const Payload = @ptrCast(@alignCast(self.payload));
        const get = payload.vtable.remote_executor orelse return null;
        return Boundary.call("remote_executor", payload.dispatch, get, .{payload.ptr});
    }
    /// Inspect support without running a potentially yielding CPU checkpoint.
    /// This does not grant authority to execute or wait; locked providers use
    /// it only to reject unsupported suspension after checking lifetime.
    pub fn hasExecutionWait(self: Port) !bool {
        try self.validate();
        const payload: *const Payload = @ptrCast(@alignCast(self.payload));
        if (!payload.vtable_contract.matches(.of(VTable))) return error.UnsupportedVersion;
        return payload.execution_wait != null;
    }
    /// Only providers that perform physical I/O may use this boundary.
    pub fn executionWait(self: Port) !?@import("execution_wait.zig").Boundary {
        try self.checkpoint();
        const payload: *const Payload = @ptrCast(@alignCast(self.payload));
        return payload.execution_wait;
    }
    /// Physical providers may inspect their explicit wait boundary without a
    /// CPU checkpoint while retaining unrelated service ownership. This does
    /// not establish dependency independence: the provider must prove that its
    /// I/O producer cannot need those resources. A restricted capture port
    /// still returns null. The original lifetime and boundary remain borrowed.
    pub fn executionWaitAfterLifetime(self: Port) !?@import("execution_wait.zig").Boundary {
        try self.checkLifetime();
        const payload: *const Payload = @ptrCast(@alignCast(self.payload));
        return payload.execution_wait;
    }
    pub fn waitForProgress(self: Port) !void {
        try self.checkpoint();
        const payload: *const Payload = @ptrCast(@alignCast(self.payload));
        const wait = payload.vtable.wait_for_progress orelse return error.ExecutionWaitUnsupported;
        try Boundary.call("wait_for_progress", payload.dispatch, wait, .{payload.ptr});
        try self.checkpoint();
    }
};

/// Preserve an original control error across a checked callback dispatcher.
/// The relay returns a catalogued cancellation through that boundary and the
/// synchronous caller restores rejection before publishing any returned owner.
/// Neither the relay nor its port may escape the call.
pub const ErrorRelay = struct {
    original: Port = undefined,
    rejection: ?anyerror = null,
    payload: Payload = undefined,

    pub fn bind(self: *ErrorRelay, original: Port) !Port {
        _ = try original.hasExecutionWait();
        const original_payload: *const Payload = @ptrCast(@alignCast(original.payload));
        self.original = original;
        self.rejection = null;
        self.payload = .{ .ptr = self, .vtable = &.{ .checkpoint = checkpoint, .check_lifetime = lifetime, .wait_for_progress = wait, .deadline_ns = deadline, .retirement_binding = retirement, .remote_executor = remoteExecutor }, .execution_wait = original_payload.execution_wait };
        return Port.init(&self.payload);
    }
    fn source(raw: *anyopaque) !*ErrorRelay {
        const self: *ErrorRelay = @ptrCast(@alignCast(raw));
        if (self.rejection != null) return error.Canceled;
        return self;
    }
    fn reject(self: *ErrorRelay, cause: anyerror) anyerror {
        self.rejection = cause;
        return error.Canceled;
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self = try source(raw);
        self.original.checkpoint() catch |err| return self.reject(err);
    }
    fn lifetime(raw: *anyopaque) !void {
        const self = try source(raw);
        self.original.checkLifetime() catch |err| return self.reject(err);
    }
    fn wait(raw: *anyopaque) !void {
        const self = try source(raw);
        self.original.waitForProgress() catch |err| return self.reject(err);
    }
    fn deadline(raw: *anyopaque) !?u64 {
        const self = try source(raw);
        return self.original.deadlineNs() catch |err| return self.reject(err);
    }
    fn retirement(raw: *anyopaque) !?Retirement.Binding {
        const self = try source(raw);
        return self.original.retirementBinding() catch |err| return self.reject(err);
    }
    fn remoteExecutor(raw: *anyopaque) !?HttpExecutor {
        const self = try source(raw);
        return self.original.remoteExecutor() catch |err| return self.reject(err);
    }
};

/// Synchronous authority while a caller retains a service dependency (for
/// example a statement fence). Preserve lifetime and clock, but never hand off
/// CPU or wait for a peer until the caller has retired every partial capture.
/// Neither this wrapper nor its port may escape the synchronous call.
pub const RetainedDependency = struct {
    original: ?Port = null,
    rejection: ?anyerror = null,
    payload: Payload = undefined,

    pub fn bind(self: *RetainedDependency, original: ?Port) ?Port {
        self.original = original;
        self.rejection = null;
        if (original == null) return null;
        self.payload = .{ .ptr = self, .vtable = &.{ .checkpoint = lifetime, .check_lifetime = lifetime, .wait_for_progress = busy, .deadline_ns = deadline, .retirement_binding = retirement, .remote_executor = remoteExecutor } };
        return Port.init(&self.payload);
    }
    fn lifetime(raw: *anyopaque) !void {
        const self: *RetainedDependency = @ptrCast(@alignCast(raw));
        if (self.rejection) |err| return err;
        self.original.?.checkLifetime() catch |err| {
            self.rejection = err;
            return err;
        };
    }
    fn retirement(raw: *anyopaque) !?Retirement.Binding {
        const self: *RetainedDependency = @ptrCast(@alignCast(raw));
        try lifetime(raw);
        return self.original.?.retirementBinding() catch |err| {
            self.rejection = err;
            return err;
        };
    }
    fn remoteExecutor(raw: *anyopaque) !?HttpExecutor {
        const self: *RetainedDependency = @ptrCast(@alignCast(raw));
        try lifetime(raw);
        return self.original.?.remoteExecutor() catch |err| {
            self.rejection = err;
            return err;
        };
    }
    fn busy(raw: *anyopaque) !void {
        try lifetime(raw);
        return error.Backpressured;
    }
    fn deadline(raw: *anyopaque) !?u64 {
        const self: *RetainedDependency = @ptrCast(@alignCast(raw));
        try lifetime(raw);
        return self.original.?.deadlineNs() catch |err| {
            self.rejection = err;
            return err;
        };
    }
};

/// The adapter and its budget must outlive the synchronous admission call.
pub fn Adapter(comptime Budget: type) type {
    return struct {
        budget: Budget,
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.budget.checkpoint();
        }
        fn wait(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.budget.waitForProgress();
        }
        fn lifetime(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.budget.checkLifetime();
        }
        fn deadline(ptr: *anyopaque) !?u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.budget.deadlineNs();
        }
        fn retirement(ptr: *anyopaque) !?Retirement.Binding {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.budget.retirementBinding();
        }
        fn remoteExecutor(ptr: *anyopaque) !?HttpExecutor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.budget.remoteExecutor();
        }
        pub fn payload(self: *@This()) Payload {
            const T = switch (@typeInfo(Budget)) {
                .pointer => |pointer| pointer.child,
                else => Budget,
            };
            return .{ .ptr = self, .vtable = &.{ .checkpoint = checkpoint, .wait_for_progress = wait, .check_lifetime = if (@hasDecl(T, "checkLifetime")) lifetime else null, .deadline_ns = if (@hasDecl(T, "deadlineNs")) deadline else null, .retirement_binding = if (@hasDecl(T, "retirementBinding")) retirement else null, .remote_executor = if (@hasDecl(T, "remoteExecutor")) remoteExecutor else null }, .execution_wait = if (@hasDecl(T, "executionBoundary")) self.budget.executionBoundary() else null };
        }
    };
}

/// A synchronous generic-budget API must not re-erase an already erased port:
/// doing so can discard its checked execution boundary or original contract.
/// The fallback adapter and budget remain borrowed only for this call.
pub fn portForBudget(budget: anytype, fallback: *const Payload) Port {
    if (comptime @TypeOf(budget) == Port) return budget;
    if (comptime @TypeOf(budget) == *Port or @TypeOf(budget) == *const Port) return budget.*;
    return Port.init(fallback);
}

/// Acquire a publication read lock without retaining CPU while a writer owns
/// it. A successful return owns the lock and has revalidated original admission.
pub fn lockShared(lock: anytype, control: Port) !void {
    try control.checkpoint();
    while (!lock.tryLockShared()) try control.waitForProgress();
    errdefer lock.unlockShared();
    try control.checkLifetime();
}

/// Scoped metadata view over an existing transaction. The caller owns both
/// transaction and admission. Copied values remain valid until deinit even
/// when the underlying point-read backend reuses its next-get buffer.
pub fn MetadataRead(comptime Transaction: type) type {
    return struct {
        transaction: Transaction,
        control: ?Port,
        arena: @import("std").heap.ArenaAllocator,

        pub fn init(alloc: @import("std").mem.Allocator, transaction: Transaction, control: ?Port) @This() {
            return .{ .transaction = transaction, .control = control, .arena = .init(alloc) };
        }
        pub fn deinit(self: *@This()) void {
            self.arena.deinit();
            self.* = undefined;
        }
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            const bytes = if (self.control) |port| blk: {
                var values: [1]?[]const u8 = .{null};
                try self.transaction.getManySortedWithAdmission(&.{key}, &values, port);
                break :blk values[0] orelse return error.NotFound;
            } else try self.transaction.get(key);
            const owned = try self.arena.allocator().dupe(u8, bytes);
            if (self.control) |port| try port.checkpoint();
            return owned;
        }
    };
}

/// A cursor borrows admission only for one synchronous operation. Failed
/// operations may have advanced cursor scratch; callers must close the cursor.
pub const Scope = struct {
    control: ?Port = null,
    pub const Guard = struct {
        owner: *Scope,
        pub fn deinit(self: Guard) void {
            self.owner.control = null;
        }
    };
    pub fn enter(self: *Scope, control: Port) !Guard {
        if (self.control != null) return error.OperationAlreadyInProgress;
        try control.checkpoint();
        self.control = control;
        return .{ .owner = self };
    }
    pub fn checkpoint(self: *const Scope) !void {
        if (self.control) |control| try control.checkpoint();
    }
};

test "transaction routing control port cursor scope clears the original borrow on failure" {
    const std = @import("std");
    const Budget = struct {
        canceled: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            if (self.canceled) return error.Canceled;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const control = Port.init(&payload);
    var scope = Scope{};
    {
        const guard = try scope.enter(control);
        defer guard.deinit();
        budget.canceled = true;
        try std.testing.expectError(error.Canceled, scope.checkpoint());
    }
    try std.testing.expect(scope.control == null);
    try scope.checkpoint();
    try std.testing.expectError(error.Canceled, scope.enter(control));
    try std.testing.expect(scope.control == null);
}

test "transaction routing control port cursor scope rejects reentry without releasing its owner" {
    const std = @import("std");
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var budget = Budget{};
    var adapter = Adapter(*Budget){ .budget = &budget };
    const payload = adapter.payload();
    const control = Port.init(&payload);
    var scope = Scope{};
    const guard = try scope.enter(control);
    try std.testing.expectError(error.OperationAlreadyInProgress, scope.enter(control));
    try std.testing.expect(scope.control != null);
    guard.deinit();
    try std.testing.expect(scope.control == null);
}

/// Bounded in-place CPU work under the original synchronous authority.
/// Preserve the ordinary sort when no controlled caller is present.
pub fn sort(comptime T: type, items: []T, context: anytype, comptime less: anytype, control: ?Port) !void {
    const std = @import("std");
    const budget = control orelse {
        std.mem.sort(T, items, context, less);
        return;
    };
    try budget.checkpoint();
    var start = items.len / 2;
    while (start > 0) {
        start -= 1;
        try sift(T, items, context, less, start, budget);
    }
    var end = items.len;
    while (end > 1) {
        try budget.checkpoint();
        end -= 1;
        std.mem.swap(T, &items[0], &items[end]);
        try sift(T, items[0..end], context, less, 0, budget);
    }
    try budget.checkpoint();
}

fn sift(comptime T: type, items: []T, context: anytype, comptime less: anytype, start: usize, budget: Port) !void {
    const std = @import("std");
    var root = start;
    while (root < items.len / 2) {
        try budget.checkpoint();
        var child = root * 2 + 1;
        if (child + 1 < items.len and less(context, items[child], items[child + 1])) child += 1;
        if (!less(context, items[root], items[child])) return;
        std.mem.swap(T, &items[root], &items[child]);
        root = child;
    }
}

/// Preserve original callback rejection across adapters that interpret a
/// provider's NotFound as exhaustion. Synchronous borrow; never retain its port.
pub const RejectionTrace = struct {
    original: ?Port = null,
    failure: ?anyerror = null,
    payload: Payload = undefined,
    pub fn bind(self: *@This(), original: ?Port) ?Port {
        self.original = original;
        self.failure = null;
        if (original == null) return null;
        self.payload = .{ .ptr = self, .vtable = &.{ .checkpoint = checkpoint, .check_lifetime = checkLifetime, .wait_for_progress = waitForProgress, .deadline_ns = deadlineNs, .retirement_binding = retirementBinding, .remote_executor = remoteExecutor } };
        // Forward capability metadata as well as callbacks. Hiding a lending
        // boundary would bypass DocStore's retained-capture eligibility guard.
        // Invalid input stays a sticky failure without dereferencing its payload.
        if (original.?.validate()) |_| {
            const source: *const Payload = @ptrCast(@alignCast(original.?.payload));
            if (source.vtable_contract.matches(.of(VTable))) {
                if (source.execution_wait != null) self.payload.execution_wait = .{ .ptr = self, .begin = beginWait, .end = endWait };
            } else self.failure = error.UnsupportedVersion;
        } else |cause| self.failure = cause;
        return Port.init(&self.payload);
    }
    fn originalWait(self: *@This()) @import("execution_wait.zig").Boundary {
        const source: *const Payload = @ptrCast(@alignCast(self.original.?.payload));
        return source.execution_wait.?;
    }
    fn beginWait(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.failure) |cause| return cause;
        self.originalWait().beginWait() catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
    fn endWait(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        // A begun wait must always rejoin the original owner, including when
        // a lifetime rejection was observed while CPU was parked.
        self.originalWait().endWait() catch |cause| {
            if (self.failure == null) self.failure = cause;
        };
        if (self.failure) |cause| return cause;
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.failure) |cause| return cause;
        self.original.?.checkpoint() catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
    fn checkLifetime(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.failure) |cause| return cause;
        self.original.?.checkLifetime() catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
    fn waitForProgress(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.failure) |cause| return cause;
        self.original.?.waitForProgress() catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
    fn retirementBinding(raw: *anyopaque) !?Retirement.Binding {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.failure) |cause| return cause;
        return self.original.?.retirementBinding() catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
    fn deadlineNs(raw: *anyopaque) !?u64 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.failure) |cause| return cause;
        return self.original.?.deadlineNs() catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
    fn remoteExecutor(raw: *anyopaque) !?HttpExecutor {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.failure) |cause| return cause;
        return self.original.?.remoteExecutor() catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
};

/// Synchronous cross-owner relay preserving original wait and reentry failures.
pub const ExecutionErrorRelay = struct {
    errors: ErrorRelay = .{},
    original_wait: ?@import("execution_wait.zig").Boundary = null,
    pub fn bind(self: *ExecutionErrorRelay, original: Port) !Port {
        const borrowed = try self.errors.bind(original);
        self.original_wait = self.errors.payload.execution_wait;
        if (self.original_wait != null) self.errors.payload.execution_wait = .{ .ptr = self, .begin = begin, .end = end };
        return borrowed;
    }
    fn reject(self: *ExecutionErrorRelay, cause: anyerror) anyerror {
        self.errors.rejection = self.errors.rejection orelse cause;
        return error.Canceled;
    }
    fn begin(raw: *anyopaque) !void {
        const self: *ExecutionErrorRelay = @ptrCast(@alignCast(raw));
        if (self.errors.rejection != null) return error.Canceled;
        self.original_wait.?.beginWait() catch |cause| return self.reject(cause);
    }
    fn end(raw: *anyopaque) !void {
        const self: *ExecutionErrorRelay = @ptrCast(@alignCast(raw));
        // Do not gate reentry on a prior rejection: the original caller still
        // owns this balanced wait pair and must get its grant back or its own
        // reentry failure before any retirement callback can run.
        self.original_wait.?.endWait() catch |cause| return self.reject(cause);
    }
};
