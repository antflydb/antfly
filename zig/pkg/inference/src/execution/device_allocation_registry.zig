// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Existing authored device allocator roots. Pins independent metadata only;
//! never a context, SDK object, Model, allocator borrow or physical payload.
//! Values are per root and include concurrent users. No independent peaks sum.
const std = @import("std");
const observer = @import("allocation_observer.zig");

pub const Root = struct {
    state: *observer.State,
    backing: enum { cuda_successful_cuMemAlloc_owned_spans },
};
pub const Report = struct {
    identity: usize = 0,
    backing: @FieldType(Root, "backing") = .cuda_successful_cuMemAlloc_owned_spans,
    scope: enum { registered_root_commit_interval_including_concurrent_users } = .registered_root_commit_interval_including_concurrent_users,
    semantic_lifetime_classes: enum { exact_kv_owning_lifetime_with_explicit_unclassified_remainder } = .exact_kv_owning_lifetime_with_explicit_unclassified_remainder,
    /// OwnedPurpose enum order; each peak is directly measured on this root.
    /// These class peaks are never summed to obtain total interval_peak_bytes.
    purpose_live_bytes: [@import("storage_accounting.zig").owned_purpose_count]u64 = @splat(0),
    purpose_interval_peak_bytes: [@import("storage_accounting.zig").owned_purpose_count]u64 = @splat(0),
    purpose_lifetime_peak_bytes: [@import("storage_accounting.zig").owned_purpose_count]u64 = @splat(0),
    live_bytes: u64 = 0,
    interval_peak_bytes: u64 = 0,
    lifetime_peak_bytes: u64 = 0,
    allocations: u64 = 0,
    frees: u64 = 0,
    /// Custody transfer changes root live bytes, never alloc/free counters.
    handoffs_in: u64 = 0,
    handoffs_out: u64 = 0,
};
pub const Summary = struct {
    coverage: enum { registered_authored_roots_only_vendor_private_unavailable } = .registered_authored_roots_only_vendor_private_unavailable,
    reports: [8]Report = @splat(.{}),
    count: u8 = 0,
    roots: usize = 0,
    truncated: bool = false,
    metadata_failures: usize = 0,
};
threadlocal var active: ?*Registry = null;
pub inline fn enabled() bool {
    return active != null;
}
pub fn missing() void {
    var cursor = active;
    while (cursor) |item| : (cursor = item.previous) item.summary.metadata_failures += 1;
}
pub fn bind(root: Root) void {
    var cursor = active;
    while (cursor) |item| : (cursor = item.previous) item.add(root);
}
pub const Registry = struct {
    const Entry = struct { root: Root, interval: *observer.State.Interval };
    previous: ?*Registry = null,
    entries: [8]Entry = undefined,
    intervals: [8]observer.State.Interval = undefined,
    count: usize = 0,
    overflow: std.ArrayList(Entry) = .empty,
    metadata: std.mem.Allocator,
    summary: Summary = .{},
    attached: bool = false,
    /// Final-address storage until end, like the host inline8 registry.
    pub fn begin(self: *Registry, metadata: std.mem.Allocator) void {
        self.* = .{ .metadata = metadata, .previous = active, .attached = true };
        active = self;
    }
    fn add(self: *Registry, root: Root) void {
        for (self.entries[0..self.count]) |entry| if (entry.root.state == root.state) return;
        for (self.overflow.items) |entry| if (entry.root.state == root.state) return;
        if (self.count < self.entries.len) {
            const index = self.count;
            root.state.beginIntervalInto(&self.intervals[index]);
            self.entries[index] = .{ .root = root, .interval = &self.intervals[index] };
            self.count += 1;
        } else {
            const interval = root.state.beginInterval() catch {
                self.summary.metadata_failures += 1;
                return;
            };
            self.overflow.append(self.metadata, .{ .root = root, .interval = interval }) catch {
                _ = interval.finish();
                self.summary.metadata_failures += 1;
            };
        }
    }
    fn captureEntry(summary: *Summary, entry: Entry, value: observer.State.Interval.Result) void {
        summary.roots += 1;
        if (summary.count == summary.reports.len) {
            summary.truncated = true;
            return;
        }
        summary.reports[summary.count] = .{
            .identity = @intFromPtr(entry.root.state),
            .purpose_live_bytes = value.usage.purpose_live_bytes,
            .purpose_interval_peak_bytes = value.purpose_peak,
            .purpose_lifetime_peak_bytes = value.usage.purpose_lifetime_peak_bytes,
            .backing = entry.root.backing,
            .live_bytes = value.usage.requested_live_bytes,
            .interval_peak_bytes = value.peak,
            .lifetime_peak_bytes = value.usage.requested_lifetime_peak_bytes,
            .allocations = value.usage.allocations,
            .frees = value.usage.frees,
            .handoffs_in = value.usage.custody_transfers_in,
            .handoffs_out = value.usage.custody_transfers_out,
        };
        summary.count += 1;
    }
    pub fn snapshot(self: *const Registry) Summary {
        var result = self.summary;
        for (self.entries[0..self.count]) |entry| captureEntry(&result, entry, entry.interval.snapshot());
        for (self.overflow.items) |entry| captureEntry(&result, entry, entry.interval.snapshot());
        return result;
    }
    pub fn end(self: *Registry) Summary {
        std.debug.assert(self.attached and active == self);
        active = self.previous;
        self.attached = false;
        var result = self.summary;
        for (self.entries[0..self.count]) |entry| captureEntry(&result, entry, entry.interval.finish());
        for (self.overflow.items) |entry| captureEntry(&result, entry, entry.interval.finish());
        self.overflow.deinit(self.metadata);
        return result;
    }
};

test "device root intervals use inline metadata and survive original ledger owner release" {
    const a = std.testing.allocator;
    const state = try observer.State.createExternal(a, .{ .requested_live_bytes = 7, .requested_lifetime_peak_bytes = 7 });
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var outer: Registry = undefined;
    outer.begin(failing.allocator());
    bind(.{ .state = state, .backing = .cuda_successful_cuMemAlloc_owned_spans });
    var inner: Registry = undefined;
    inner.begin(failing.allocator());
    bind(.{ .state = state, .backing = .cuda_successful_cuMemAlloc_owned_spans });
    state.commitExternal(.{ .requested_live_bytes = 31, .requested_lifetime_peak_bytes = 31, .allocations = 1 });
    state.commitExternal(.{ .requested_live_bytes = 2, .requested_lifetime_peak_bytes = 31, .allocations = 1, .frees = 1 });
    state.release(); // both subscriptions independently retain metadata
    const child = inner.end();
    const parent = outer.end();
    try std.testing.expectEqual(@as(u64, 31), child.reports[0].interval_peak_bytes);
    try std.testing.expectEqual(@as(u64, 2), parent.reports[0].live_bytes);
    try std.testing.expectEqual(@as(u64, 1), parent.reports[0].frees);
    try std.testing.expect(!failing.has_induced_failure);
}

test "device root overflow interval OOM and registry append OOM preserve eight inline roots" {
    const a = std.testing.allocator;
    var roots: [9]*observer.State = undefined;
    var initialized: usize = 0;
    defer for (roots[0..initialized]) |state| state.release();
    for (&roots) |*slot| {
        slot.* = try observer.State.createExternal(a, .{});
        initialized += 1;
    }
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var registry: Registry = undefined;
    registry.begin(failing.allocator());
    for (roots) |state| bind(.{ .state = state, .backing = .cuda_successful_cuMemAlloc_owned_spans });
    const result = registry.end();
    try std.testing.expectEqual(@as(usize, 8), result.roots);
    try std.testing.expectEqual(@as(usize, 1), result.metadata_failures);
    try std.testing.expect(failing.has_induced_failure);
    // A different ninth owner can also refuse its heap node allocation. The
    // first eight still register without touching their metadata allocator.
    var node_failure = std.testing.FailingAllocator.init(a, .{ .fail_index = 1 });
    const ninth = try observer.State.createExternal(node_failure.allocator(), .{});
    defer ninth.release();
    registry.begin(a);
    for (roots[0..8]) |state| bind(.{ .state = state, .backing = .cuda_successful_cuMemAlloc_owned_spans });
    bind(.{ .state = ninth, .backing = .cuda_successful_cuMemAlloc_owned_spans });
    const refused = registry.end();
    try std.testing.expectEqual(@as(usize, 8), refused.roots);
    try std.testing.expectEqual(@as(usize, 1), refused.metadata_failures);
    try std.testing.expect(node_failure.has_induced_failure);
}
