// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Synchronous admission borrow. Never retained by a returned read transaction.
const native = @import("antfly_runtime_abi").native_abi;
const callback = @import("../runtime_callback_abi.zig");
pub const VTable = struct {
    checkpoint: *const fn (*anyopaque) anyerror!void,
    wait_for_progress: ?*const fn (*anyopaque) anyerror!void = null,
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
    /// Only providers that perform physical I/O may use this boundary.
    pub fn executionWait(self: Port) !?@import("execution_wait.zig").Boundary {
        try self.checkpoint();
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
        pub fn payload(self: *@This()) Payload {
            const T = switch (@typeInfo(Budget)) {
                .pointer => |pointer| pointer.child,
                else => Budget,
            };
            return .{ .ptr = self, .vtable = &.{ .checkpoint = checkpoint, .wait_for_progress = wait }, .execution_wait = if (@hasDecl(T, "executionBoundary")) self.budget.executionBoundary() else null };
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
