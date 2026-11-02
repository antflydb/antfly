// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Optional submitting-thread allocation owner intervals. Entries retain only
//! independent observer metadata, never temporary compute/backend pointers.
const std = @import("std");
const observer = @import("allocation_observer.zig");
const platform = @import("antfly_platform");

pub const Role = enum { model, compute };
pub const OwnerReport = struct {
    owner_identity: usize = 0,
    parent_identity: ?usize = null,
    model: bool = false,
    compute: bool = false,
    covered_by_registered_ancestor: bool = false,
    coverage: observer.Coverage = .since_enable,
    counter_scope: enum { owner_interval } = .owner_interval,
    /// Current/lifetime gauges and this observer's actual counter interval.
    usage: observer.Snapshot = .{},
};

pub const Summary = struct {
    /// This is never a census of uninstrumented libraries or all process heaps.
    scope: enum { registered_allocator_owners } = .registered_allocator_owners,
    owners: usize = 0,
    model_owners: usize = 0,
    compute_owners: usize = 0,
    observed_roots: usize = 0,
    reports: [8]OwnerReport = @splat(.{}),
    reports_count: u8 = 0,
    reports_truncated: bool = false,
    /// Aggregates are available only for one proven observer root. Distinct
    /// opaque backing allocators do not prove independent payload ownership.
    aggregate_owner_identity: ?usize = null,
    since_enable_owners: usize = 0,
    missing_model_registrations: usize = 0,
    missing_compute_registrations: usize = 0,
    metadata_failures: usize = 0,
    /// Single proven root's live requested payloads; excludes mappings and
    /// unobserved owners. Never sum independent/possibly overlapping roots.
    requested_live_bytes: ?u64 = null,
    /// Largest single owner's lifetime peak; not an invocation high water and
    /// never a sum of independently timed owner peaks.
    maximum_individual_lifetime_peak_bytes: ?u64 = null,
    allocations: ?u64 = null,
    frees: ?u64 = null,
    resizes: ?u64 = null,
    remaps: ?u64 = null,
    unobserved_frees: ?u64 = null,
    unobserved_resizes: ?u64 = null,
    unobserved_remaps: ?u64 = null,
};

threadlocal var active: ?*Registry = null;

pub inline fn enabled() bool {
    return active != null;
}

pub fn bind(role: Role, owner: *observer.State) void {
    var current = active;
    while (current) |registry| : (current = registry.previous) registry.add(role, owner);
}

pub fn missing(role: Role) void {
    var current = active;
    while (current) |registry| : (current = registry.previous) {
        switch (role) {
            .model => registry.summary.missing_model_registrations += 1,
            .compute => registry.summary.missing_compute_registrations += 1,
        }
    }
}

pub const Registry = struct {
    const Entry = struct {
        owner: *observer.State,
        before: observer.Snapshot,
        model: bool = false,
        compute: bool = false,
    };
    previous: ?*Registry = null,
    inline_entries: [8]Entry = undefined,
    inline_count: usize = 0,
    overflow: std.ArrayList(Entry) = .empty,
    metadata: std.mem.Allocator,
    summary: Summary = .{},
    attached: bool = false,

    /// Call after placing this Registry at its final stack address.
    pub fn begin(self: *Registry, metadata: std.mem.Allocator) void {
        self.* = .{ .metadata = metadata, .previous = active, .attached = true };
        active = self;
    }

    fn mark(entry: *Entry, role: ?Role) void {
        switch (role orelse return) {
            .model => entry.model = true,
            .compute => entry.compute = true,
        }
    }
    fn add(self: *Registry, role: ?Role, owner: *observer.State) void {
        // Register ancestry first so its interval baseline precedes every
        // child interval later excluded from the disjoint owner union.
        if (owner.parent()) |parent| self.add(null, parent);
        for (self.inline_entries[0..self.inline_count]) |*entry| if (entry.owner == owner) {
            mark(entry, role);
            return;
        };
        for (self.overflow.items) |*entry| if (entry.owner == owner) {
            mark(entry, role);
            return;
        };
        var entry = Entry{ .owner = owner, .before = owner.snapshot() };
        mark(&entry, role);
        if (self.inline_count < self.inline_entries.len) {
            self.inline_entries[self.inline_count] = entry;
            self.inline_count += 1;
        } else self.overflow.append(self.metadata, entry) catch {
            self.summary.metadata_failures += 1;
            return;
        };
        _ = owner.retain();
    }
    fn contains(self: *const Registry, owner: *observer.State) bool {
        for (self.inline_entries[0..self.inline_count]) |entry| if (entry.owner == owner) return true;
        for (self.overflow.items) |entry| if (entry.owner == owner) return true;
        return false;
    }
    fn aggregate(self: *Registry, entry: Entry) void {
        self.summary.owners += 1;
        self.summary.model_owners += @intFromBool(entry.model);
        self.summary.compute_owners += @intFromBool(entry.compute);
        self.summary.since_enable_owners += @intFromBool(entry.owner.coverage == .since_enable);
        var parent_owner = entry.owner.parent();
        var covered = false;
        // An ancestor relationship is authoritative. An opaque allocator can
        // hide another observer, so distinct roots alone never prove disjoint.
        while (parent_owner) |parent| : (parent_owner = parent.parent()) if (self.contains(parent)) {
            covered = true;
            break;
        };
        const after = entry.owner.snapshot();
        const interval = after.since(entry.before);
        self.summary.maximum_individual_lifetime_peak_bytes = @max(self.summary.maximum_individual_lifetime_peak_bytes orelse 0, after.requested_lifetime_peak_bytes);
        if (self.summary.reports_count < self.summary.reports.len) {
            self.summary.reports[self.summary.reports_count] = .{
                .owner_identity = @intFromPtr(entry.owner),
                .parent_identity = if (entry.owner.parent()) |parent| @intFromPtr(parent) else null,
                .model = entry.model,
                .compute = entry.compute,
                .covered_by_registered_ancestor = covered,
                .coverage = entry.owner.coverage,
                .usage = interval,
            };
            self.summary.reports_count += 1;
        } else self.summary.reports_truncated = true;
        if (covered) return;
        self.summary.observed_roots += 1;
        const single_root = self.summary.observed_roots == 1;
        self.summary.aggregate_owner_identity = if (single_root) @intFromPtr(entry.owner) else null;
        self.summary.requested_live_bytes = if (single_root) after.requested_live_bytes else null;
        inline for (.{ "allocations", "frees", "resizes", "remaps", "unobserved_frees", "unobserved_resizes", "unobserved_remaps" }) |name| {
            @field(self.summary, name) = if (single_root) @field(interval, name) else null;
        }
    }

    pub fn finish(self: *Registry) Summary {
        std.debug.assert(self.attached and active == self);
        active = self.previous;
        self.attached = false;
        for (self.inline_entries[0..self.inline_count]) |entry| self.aggregate(entry);
        for (self.overflow.items) |entry| self.aggregate(entry);
        for (self.inline_entries[0..self.inline_count]) |entry| entry.owner.release();
        for (self.overflow.items) |entry| entry.owner.release();
        self.overflow.deinit(self.metadata);
        return self.summary;
    }
};

pub fn metadataAllocator() std.mem.Allocator {
    return platform.allocator.processAllocator(platform.allocator.concurrentFallback());
}

test "observed owner intervals deduplicate identities and nested allocator payloads" {
    const a = std.testing.allocator;
    const parent = try observer.State.createWithMetadata(a, a, .from_owner_creation);
    defer parent.release();
    const child = try observer.State.createWithMetadata(parent.allocator(), a, .from_owner_creation);
    defer child.release();
    var registry: Registry = undefined;
    registry.begin(a);
    bind(.model, parent);
    bind(.compute, child);
    bind(.compute, parent);
    const bytes = try child.allocator().alloc(u8, 31);
    child.allocator().free(bytes);
    const measured = registry.finish();
    try std.testing.expectEqual(@as(usize, 2), measured.owners);
    try std.testing.expectEqual(@as(usize, 1), measured.observed_roots);
    try std.testing.expectEqual(@as(usize, 1), measured.model_owners);
    try std.testing.expectEqual(@as(usize, 2), measured.compute_owners);
    try std.testing.expectEqual(@as(u64, 1), measured.allocations.?);
    try std.testing.expectEqual(@as(u64, 1), measured.frees.?);
    try std.testing.expectEqual(@as(u64, 31), measured.maximum_individual_lifetime_peak_bytes.?);
}

test "nested observed registries retain retired metadata and unwind independently" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    const owner = try observer.State.createWithMetadata(arena.allocator(), a, .since_enable);
    var outer: Registry = undefined;
    outer.begin(a);
    var inner: Registry = undefined;
    inner.begin(a);
    bind(.compute, owner);
    const bytes = try owner.allocator().alloc(u8, 17);
    owner.allocator().free(bytes);
    owner.release();
    arena.deinit();
    const child = inner.finish();
    try std.testing.expect(enabled());
    const parent = outer.finish();
    try std.testing.expect(!enabled());
    try std.testing.expectEqual(@as(u64, 1), child.allocations.?);
    try std.testing.expectEqual(child.allocations.?, parent.allocations.?);
    try std.testing.expectEqual(@as(u64, 17), parent.maximum_individual_lifetime_peak_bytes.?);
}

test "registry metadata allocation failure reports partial coverage without failing inference" {
    const a = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var registry: Registry = undefined;
    registry.begin(failing.allocator());
    var owners: [9]*observer.State = undefined;
    var count: usize = 0;
    defer for (owners[0..count]) |owner| owner.release();
    for (&owners) |*owner| {
        owner.* = try observer.State.createWithMetadata(a, a, .since_enable);
        count += 1;
        bind(.compute, owner.*);
    }
    const summary = registry.finish();
    try std.testing.expectEqual(@as(usize, 8), summary.owners);
    try std.testing.expectEqual(@as(usize, 1), summary.metadata_failures);
}

test "multiple opaque observer roots keep exact owner intervals without inventing disjoint totals" {
    const a = std.testing.allocator;
    const first = try observer.State.createWithMetadata(a, a, .from_owner_creation);
    defer first.release();
    const second = try observer.State.createWithMetadata(a, a, .from_owner_creation);
    defer second.release();
    var registry: Registry = undefined;
    registry.begin(a);
    bind(.model, first);
    bind(.compute, second);
    const x = try first.allocator().alloc(u8, 11);
    defer first.allocator().free(x);
    const y = try second.allocator().alloc(u8, 17);
    defer second.allocator().free(y);
    const measured = registry.finish();
    try std.testing.expectEqual(@as(usize, 2), measured.observed_roots);
    try std.testing.expect(measured.aggregate_owner_identity == null);
    try std.testing.expect(measured.requested_live_bytes == null and measured.allocations == null);
    try std.testing.expectEqual(@as(u8, 2), measured.reports_count);
    try std.testing.expectEqual(@as(u64, 11), measured.reports[0].usage.requested_live_bytes);
    try std.testing.expectEqual(@as(u64, 17), measured.reports[1].usage.requested_live_bytes);
    try std.testing.expectEqual(@as(u64, 17), measured.maximum_individual_lifetime_peak_bytes.?);
}

test "bounded owner reports own all data after sampled states and overflow metadata retire" {
    const a = std.testing.allocator;
    var registry: Registry = undefined;
    registry.begin(a);
    for (0..9) |_| {
        const owner = try observer.State.createWithMetadata(a, a, .since_enable);
        bind(.compute, owner);
        owner.release();
    }
    const measured = registry.finish();
    const copied = measured;
    try std.testing.expectEqual(@as(usize, 9), copied.owners);
    try std.testing.expectEqual(@as(u8, 8), copied.reports_count);
    try std.testing.expect(copied.reports_truncated);
    try std.testing.expectEqual(@as(usize, 0), copied.metadata_failures);
    try std.testing.expect(copied.reports[0].owner_identity != 0);
    try std.testing.expectEqual(@as(u64, 0), copied.reports[0].usage.requested_live_bytes);
}
