// Copyright 2026 Antfly, Inc.
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
const types = @import("types.zig");

pub const QueryVisibilityChange = enum {
    invalidate,
    status,
    /// High-frequency, readiness-neutral owner telemetry. Serving layers may
    /// coalesce this independently from durable status and visibility edges.
    activity,
    publish,
    publish_consistent,
    publish_blocking,
    /// The primary source replay target advanced at a durable commit
    /// boundary. This is convergence-only: it must not revoke an already
    /// published serving generation.
    target_advanced,
    index_repair_pending,
    index_repair_cleared,
    /// An exact derived watermark advanced far enough to wake a resident
    /// progress wait. This is scheduling-only: it neither invalidates readers
    /// nor changes durable repair admission.
    index_repair_progress,
};

pub const IndexRepairAdmission = enum {
    unknown,
    serviceable,
    blocked,
};

/// Durable lifecycle class carried with visibility edges. Consumers must use
/// this fact instead of interpreting trigger strings: initial materialization
/// shares the generation scheduler with repair but is not corruption debt.
pub const IndexLifecycleWorkClass = enum {
    repair,
    initial_build,
};

/// Incarnation-scoped identity for a durable repair visibility edge. The
/// slices are borrowed for the synchronous notification only; consumers that
/// retain an event must clone them.
pub const IndexRepairVisibility = struct {
    index_name: []const u8,
    work_class: IndexLifecycleWorkClass = .repair,
    repair_id: u128 = 0,
    revision: u64 = 0,
    config_hash: u64 = 0,
    root_generation: u64 = 0,
    previous_admission: IndexRepairAdmission = .unknown,
    admission: IndexRepairAdmission = .unknown,
    previous_action_required: bool = false,
    action_required: bool = false,
};

pub const IndexTargetVisibility = types.IndexTargetVisibility;

pub const QueryVisibilityEvent = struct {
    change: QueryVisibilityChange,
    repair: ?IndexRepairVisibility = null,
    target_sequence: ?u64 = null,
    target_indexes: []const IndexTargetVisibility = &.{},
    target_scope_known: bool = false,
    /// An exact clear may leave other durable lifecycle work in the group.
    /// This is a scheduling/replay fact, not an unknown-scope visibility
    /// edge: consumers must audit the group without fencing unrelated index
    /// incarnations.
    group_repair_debt_remains: bool = false,
};

pub const QueryVisibilityHook = struct {
    ptr: *anyopaque,
    on_change: *const fn (ptr: *anyopaque, event: QueryVisibilityEvent) void,
    pub fn notify(self: @This(), event: QueryVisibilityEvent) void {
        self.on_change(self.ptr, event);
    }
};

/// Owns borrowed callback attachment and its teardown barrier. Replay identity
/// is reconstructed by DB from durable state, never retained in this owner.
pub const Observer = struct {
    mutex: std.atomic.Mutex = .unlocked,
    hook: ?QueryVisibilityHook = null,
    in_flight: std.atomic.Value(u32) = .init(0),
    replay_pending: bool = false,
    pub const Lease = struct {
        owner: *Observer,
        hook: QueryVisibilityHook,
        pub fn release(self: Lease) void {
            _ = self.owner.in_flight.fetchSub(1, .release);
        }
    };
    /// A replay lease must be released after synchronous durable rehydration.
    /// As before, detaching from inside one's own callback is not supported.
    pub fn attach(self: *Observer, hook: ?QueryVisibilityHook) ?Lease {
        var replay: ?Lease = null;
        lockAtomic(&self.mutex);
        self.hook = hook;
        if (hook) |new_hook| {
            if (self.replay_pending) {
                self.replay_pending = false;
                _ = self.in_flight.fetchAdd(1, .acquire);
                replay = .{ .owner = self, .hook = new_hook };
            }
        }
        self.mutex.unlock();
        if (hook == null) while (self.in_flight.load(.acquire) != 0) {
            spinOrYield();
        };
        return replay;
    }
    pub fn attached(self: *Observer) bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.hook != null;
    }
    pub fn notify(self: *Observer, event: QueryVisibilityEvent) void {
        lockAtomic(&self.mutex);
        const hook = self.hook orelse {
            switch (event.change) {
                .index_repair_pending => self.replay_pending = true,
                .index_repair_cleared => if (event.repair != null) {
                    // An exact clear carries enough aggregate information to
                    // settle or retain the hook-attachment replay bit. A
                    // legacy/unknown clear cannot prove that queued debt is
                    // gone and therefore leaves the bit conservative.
                    self.replay_pending = event.group_repair_debt_remains;
                },
                .index_repair_progress => {},
                else => {},
            }
            self.mutex.unlock();
            return;
        };
        if (event.change == .index_repair_pending or event.change == .index_repair_cleared) {
            // A delivered exact clear settles its own edge, but it may also be
            // the only bounded notification that another durable intent
            // remains. Retain the replay bit for a later hook attachment
            // without manufacturing an anonymous visibility invalidation.
            self.replay_pending = event.group_repair_debt_remains;
        }
        _ = self.in_flight.fetchAdd(1, .acquire);
        self.mutex.unlock();
        defer _ = self.in_flight.fetchSub(1, .release);
        hook.notify(event);
    }
};
fn spinOrYield() void {
    if (@import("builtin").os.tag == .freestanding) std.atomic.spinLoopHint() else @import("antfly_platform").time.yieldNow();
}
fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) spinOrYield();
}

test "visibility observer retains exact debt for attachment replay without borrowed identity" {
    var observer: Observer = .{};
    const F = struct {
        calls: usize = 0,
        observer: *Observer,
        fn notify(ptr: *anyopaque, _: QueryVisibilityEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            // Callback inspection must not run under the attachment mutex.
            std.debug.assert(self.observer.attached());
        }
    };
    var f: F = .{ .observer = &observer };
    const hook: QueryVisibilityHook = .{ .ptr = &f, .on_change = F.notify };
    observer.notify(.{ .change = .index_repair_pending });
    observer.notify(.{ .change = .index_repair_cleared });
    const lease = observer.attach(hook).?;
    try std.testing.expectEqual(@as(u32, 1), observer.in_flight.load(.acquire));
    lease.release();
    observer.notify(.{ .change = .index_repair_cleared, .repair = .{ .index_name = "index" }, .group_repair_debt_remains = true });
    try std.testing.expectEqual(@as(usize, 1), f.calls);
    try std.testing.expect(observer.attach(null) == null);
    try std.testing.expect(!observer.attached());
    const replay = observer.attach(hook).?;
    replay.release();
    observer.notify(.{ .change = .index_repair_cleared, .repair = .{ .index_name = "index" } });
    try std.testing.expect(observer.attach(null) == null);
    try std.testing.expect(observer.attach(hook) == null);
    try std.testing.expect(observer.attach(null) == null);
}

test "visibility observer detach waits for an active callback outside the mutex" {
    const builtin = @import("builtin");
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const F = struct {
        observer: Observer = .{},
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        detached: std.atomic.Value(bool) = .init(false),
        fn notify(ptr: *anyopaque, _: QueryVisibilityEvent) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) spinOrYield();
        }
        fn send(self: *@This()) void {
            self.observer.notify(.{ .change = .status });
        }
        fn detach(self: *@This()) void {
            _ = self.observer.attach(null);
            self.detached.store(true, .release);
        }
    };
    var f: F = .{};
    _ = f.observer.attach(.{ .ptr = &f, .on_change = F.notify });
    const sender = try std.Thread.spawn(.{}, F.send, .{&f});
    defer sender.join();
    defer f.release.store(true, .release);
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!f.entered.load(.acquire) and @import("antfly_platform").time.monotonicNs() < deadline) spinOrYield();
    try std.testing.expect(f.entered.load(.acquire));
    const detacher = try std.Thread.spawn(.{}, F.detach, .{&f});
    defer detacher.join();
    // This defer precedes the detacher join even if an assertion fails.
    defer f.release.store(true, .release);
    while (f.observer.attached() and @import("antfly_platform").time.monotonicNs() < deadline) spinOrYield();
    try std.testing.expect(!f.observer.attached());
    try std.testing.expect(!f.detached.load(.acquire));
    f.release.store(true, .release);
}
