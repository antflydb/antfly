// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Process-local exclusion for a bounded set of durable completion obligations.
//! This does not persist authority or reserve storage. The durable slot owner
//! installs/restores the transaction before admitting mutations, and retires
//! it only after durable completion. An unknown outcome must retain the fence.
const std = @import("std");

pub const TxnId = [16]u8;

pub const Fence = struct {
    mutex: std.atomic.Mutex = .unlocked,
    owners: [4]?TxnId = @splat(null),
    transitions: usize = 0,

    fn lock(self: *Fence) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// Caller holds DB apply serialization while validating the profile and
    /// publishing its prepare. Same-identity restore/retry is idempotent.
    pub fn begin(self: *Fence, txn_id: TxnId) !void {
        self.lock();
        defer self.mutex.unlock();
        for (self.owners) |maybe| if (maybe) |owner| {
            if (std.mem.eql(u8, &owner, &txn_id)) return;
        };
        if (self.transitions != 0) return error.CompletionTransitionInProgress;
        for (&self.owners) |*owner| if (owner.* == null) {
            owner.* = txn_id;
            return;
        };
        return error.PreparedCompletionActive;
    }

    /// Invoke before startup workers or other table mutations become visible.
    pub fn restore(self: *Fence, txn_id: TxnId) !void {
        try self.begin(txn_id);
    }

    pub fn retire(self: *Fence, txn_id: TxnId) !void {
        self.lock();
        defer self.mutex.unlock();
        for (&self.owners) |*maybe| if (maybe.*) |owner| {
            if (std.mem.eql(u8, &owner, &txn_id)) {
                maybe.* = null;
                return;
            }
        };
        return error.CompletionFenceIdentityMismatch;
    }

    /// A diagnostic check alone does not cover a later mutation. Callers that
    /// may drop apply/catalog locks must hold beginTransition's guard instead.
    pub fn checkTransition(self: *Fence) !void {
        self.lock();
        defer self.mutex.unlock();
        for (self.owners) |owner| if (owner != null) return error.PreparedCompletionActive;
    }

    /// Fail immediately rather than waiting under an apply/catalog lock for a
    /// prepared transaction that needs that same lock to finish. Nested
    /// transition scopes are allowed; they hold no mutex across callbacks.
    pub fn beginTransition(self: *Fence) !Transition {
        self.lock();
        defer self.mutex.unlock();
        for (self.owners) |owner| if (owner != null) return error.PreparedCompletionActive;
        self.transitions = std.math.add(usize, self.transitions, 1) catch
            return error.CompletionTransitionCapacityExceeded;
        return .{ .fence = self };
    }

    pub const Transition = struct {
        fence: *Fence,
        active: bool = true,

        pub fn deinit(self: *Transition) void {
            std.debug.assert(self.active);
            self.fence.lock();
            defer self.fence.mutex.unlock();
            std.debug.assert(self.fence.transitions != 0);
            self.fence.transitions -= 1;
            self.active = false;
        }
    };
};

test "workload admission completion eligibility retains exact owner until durable retirement" {
    var fence: Fence = .{};
    const first: TxnId = @splat(1);
    const second: TxnId = @splat(2);
    try fence.checkTransition();
    try fence.begin(first);
    try fence.begin(first);
    try fence.restore(first);
    try fence.begin(second);
    try fence.retire(second);
    try std.testing.expectError(error.PreparedCompletionActive, fence.checkTransition());
    try std.testing.expectError(error.PreparedCompletionActive, fence.beginTransition());
    try std.testing.expectError(error.CompletionFenceIdentityMismatch, fence.retire(second));
    try std.testing.expectError(error.PreparedCompletionActive, fence.checkTransition());
    try fence.retire(first);
    try fence.checkTransition();
    try std.testing.expectError(error.CompletionFenceIdentityMismatch, fence.retire(first));
    try fence.restore(second);
    try std.testing.expectError(error.PreparedCompletionActive, fence.checkTransition());
    try fence.retire(second);
}

test "workload admission completion eligibility transition scopes close prepare races without nested locks" {
    var fence: Fence = .{};
    var outer = try fence.beginTransition();
    var inner = try fence.beginTransition();
    const txn: TxnId = @splat(3);
    try std.testing.expectError(error.CompletionTransitionInProgress, fence.begin(txn));
    inner.deinit();
    try std.testing.expectError(error.CompletionTransitionInProgress, fence.restore(txn));
    outer.deinit();
    try fence.begin(txn);
    try std.testing.expectError(error.PreparedCompletionActive, fence.beginTransition());
    try fence.retire(txn);
}

test "workload admission completion eligibility permits four independent obligations" {
    var fence: Fence = .{};
    for (0..4) |i| try fence.begin(@splat(@as(u8, @intCast(i))));
    try std.testing.expectError(error.PreparedCompletionActive, fence.begin(@splat(5)));
    for (0..3) |i| {
        try fence.retire(@splat(@as(u8, @intCast(i))));
        try std.testing.expectError(error.PreparedCompletionActive, fence.checkTransition());
    }
    try fence.retire(@splat(3));
    try fence.checkTransition();
}
