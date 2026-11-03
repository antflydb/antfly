// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Invocation-scoped observations of independently retained physical owners.
//! A registry pins metadata, not tensors or physical storage. Owner snapshots
//! are copied by value before releasing every pin; no callback/context escapes.
const std = @import("std");

pub const Report = struct {
    identity: usize = 0,
    phase: enum { attached_subset, detached_cleanup } = .attached_subset,
    /// Attached spans already belong to this aggregate ledger. Never add them
    /// to that ledger; detached spans have no surviving source ledger pointer.
    source_ledger_identity: ?usize = null,
    physical_live_bytes: usize = 0,
    /// Actual custody transfer, distinct from physical allocation/free events.
    handed_off_bytes: usize = 0,
    cleanup_physical_frees: ?u64 = null,
};
pub const Owner = struct {
    context: *anyopaque,
    vtable: *const VTable,
    pub const VTable = struct {
        retain: *const fn (*anyopaque) void,
        release: *const fn (*anyopaque) void,
        snapshot: *const fn (*anyopaque) Report,
    };
};
pub const Summary = struct {
    scope: enum { registered_retained_device_owners } = .registered_retained_device_owners,
    reports: [8]Report = @splat(.{}),
    report_count: u8 = 0,
    owners: usize = 0,
    reports_truncated: bool = false,
    metadata_failures: usize = 0,
    /// Registration coverage is deliberately explicit. This is neither total
    /// CUDA process memory nor a universal census of vendor-owned allocations.
    coverage: enum { registered_owners_only } = .registered_owners_only,
};
threadlocal var active: ?*Registry = null;
pub inline fn enabled() bool {
    return active != null;
}
pub fn bind(owner: Owner) void {
    var cursor = active;
    while (cursor) |registry| : (cursor = registry.previous) registry.add(owner);
}
pub const Registry = struct {
    previous: ?*Registry = null,
    entries: [8]Owner = undefined,
    count: usize = 0,
    overflow: std.ArrayList(Owner) = .empty,
    allocator: std.mem.Allocator,
    summary: Summary = .{},
    attached: bool = false,
    pub fn begin(self: *Registry, allocator: std.mem.Allocator) void {
        self.* = .{ .allocator = allocator, .previous = active, .attached = true };
        active = self;
    }
    fn add(self: *Registry, owner: Owner) void {
        for (self.entries[0..self.count]) |entry| if (entry.context == owner.context and entry.vtable == owner.vtable) return;
        for (self.overflow.items) |entry| if (entry.context == owner.context and entry.vtable == owner.vtable) return;
        if (self.count < self.entries.len) {
            self.entries[self.count] = owner;
            self.count += 1;
        } else self.overflow.append(self.allocator, owner) catch {
            self.summary.metadata_failures += 1;
            return;
        };
        owner.vtable.retain(owner.context);
    }
    fn capture(summary: *Summary, owner: Owner) void {
        const value = owner.vtable.snapshot(owner.context);
        summary.owners += 1;
        if (summary.report_count < summary.reports.len) {
            summary.reports[summary.report_count] = value;
            summary.report_count += 1;
        } else summary.reports_truncated = true;
    }
    /// A copied metadata census; retained owner pins remain attached until end.
    /// This permits a movable Diagnostics.Start to borrow a final-stack-address
    /// registry while publishing only this self-contained value.
    pub fn snapshot(self: *const Registry) Summary {
        var result = self.summary;
        for (self.entries[0..self.count]) |owner| capture(&result, owner);
        for (self.overflow.items) |owner| capture(&result, owner);
        return result;
    }
    pub fn end(self: *Registry) Summary {
        std.debug.assert(self.attached and active == self);
        active = self.previous;
        self.attached = false;
        const result = self.snapshot();
        for (self.entries[0..self.count]) |owner| owner.vtable.release(owner.context);
        for (self.overflow.items) |owner| owner.vtable.release(owner.context);
        self.overflow.deinit(self.allocator);
        return result;
    }
};

test "device owner scope pins metadata without delaying physical retirement" {
    const Fake = struct {
        refs: usize = 1,
        live: usize = 32,
        source: ?usize = 123,
        handed_off: usize = 0,
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs += 1;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs -= 1;
        }
        fn snapshot(raw: *anyopaque) Report {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .identity = @intFromPtr(self), .phase = if (self.source != null) .attached_subset else .detached_cleanup, .source_ledger_identity = self.source, .physical_live_bytes = self.live, .handed_off_bytes = self.handed_off };
        }
        const vtable = Owner.VTable{ .retain = retain, .release = release, .snapshot = snapshot };
    };
    var fake = Fake{};
    var registry: Registry = undefined;
    registry.begin(std.testing.allocator);
    bind(.{ .context = &fake, .vtable = &Fake.vtable });
    bind(.{ .context = &fake, .vtable = &Fake.vtable });
    try std.testing.expectEqual(@as(usize, 2), fake.refs);
    fake.handed_off = fake.live;
    fake.source = null;
    // Real tensor cleanup may free every byte while a metadata pin remains.
    fake.live = 0;
    const summary = registry.end();
    try std.testing.expectEqual(@as(usize, 1), fake.refs);
    try std.testing.expectEqual(@as(usize, 1), summary.owners);
    try std.testing.expectEqual(@as(usize, 0), summary.reports[0].physical_live_bytes);
    try std.testing.expectEqual(@as(usize, 32), summary.reports[0].handed_off_bytes);
    try std.testing.expectEqual(.detached_cleanup, summary.reports[0].phase);
}

test "device owner reports truncate by value and failed registry metadata never leaks owner pins" {
    const Fake = struct {
        refs: usize = 1,
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs += 1;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs -= 1;
        }
        fn snapshot(raw: *anyopaque) Report {
            return .{ .identity = @intFromPtr(raw) };
        }
        const vtable = Owner.VTable{ .retain = retain, .release = release, .snapshot = snapshot };
    };
    inline for (.{ false, true }) |fail_metadata| {
        var owners: [9]Fake = @splat(.{});
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
        var outer: Registry = undefined;
        outer.begin(if (fail_metadata) failing.allocator() else std.testing.allocator);
        var inner: Registry = undefined;
        inner.begin(std.testing.allocator);
        for (&owners) |*owner| bind(.{ .context = owner, .vtable = &Fake.vtable });
        try std.testing.expectEqual(@as(usize, if (fail_metadata) 2 else 3), owners[8].refs);
        const inner_report = inner.end();
        try std.testing.expectEqual(@as(usize, 9), inner_report.owners);
        try std.testing.expect(inner_report.reports_truncated);
        const outer_report = outer.end();
        try std.testing.expectEqual(@as(usize, if (fail_metadata) 8 else 9), outer_report.owners);
        try std.testing.expectEqual(@as(usize, if (fail_metadata) 1 else 0), outer_report.metadata_failures);
        try std.testing.expectEqual(!fail_metadata, outer_report.reports_truncated);
        for (&owners) |*owner| try std.testing.expectEqual(@as(usize, 1), owner.refs);
    }
}
