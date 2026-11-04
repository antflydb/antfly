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
    requested_owner_interval_peak_bytes: ?u64 = null,
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
    /// One proven root, registration through subscription removal inside finish,
    /// including other callers
    /// using that owner. Never a sum of independent or differently timed peaks.
    requested_owner_interval_peak_bytes: ?u64 = null,
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
        interval: ?*observer.State.Interval = null,
        model: bool = false,
        compute: bool = false,
    };
    previous: ?*Registry = null,
    inline_entries: [8]Entry = undefined,
    // Entries may be copied during aggregation, so subscriptions point to this
    // parallel final-address array rather than into a movable Entry value.
    inline_intervals: [8]observer.State.Interval = undefined,
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
        var inserted: *Entry = undefined;
        const inline_index: ?usize = if (self.inline_count < self.inline_entries.len) self.inline_count else null;
        if (inline_index) |index| {
            self.inline_entries[index] = entry;
            inserted = &self.inline_entries[index];
            self.inline_count += 1;
        } else {
            self.overflow.append(self.metadata, entry) catch {
                self.summary.metadata_failures += 1;
                return;
            };
            inserted = &self.overflow.items[self.overflow.items.len - 1];
        }
        _ = owner.retain();
        // Registry.begin already requires final-address storage. Common slots
        // borrow stable nodes without heap metadata or an optional OOM path.
        if (inline_index) |index| {
            const interval = &self.inline_intervals[index];
            owner.beginIntervalInto(interval);
            inserted.interval = interval;
        } else {
            // Overflow Entry storage may move on append. Keep each linked
            // interval independently allocated, with explicit partial coverage.
            inserted.interval = owner.beginInterval() catch {
                self.summary.metadata_failures += 1;
                return;
            };
        }
        inserted.before = inserted.interval.?.before;
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
        const finished = if (entry.interval) |value| value.finish() else null;
        const after = if (finished) |value| value.usage else entry.owner.snapshot();
        const interval = if (finished != null) after else after.since(entry.before);
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
                .requested_owner_interval_peak_bytes = if (finished) |value| value.peak else null,
            };
            self.summary.reports_count += 1;
        } else self.summary.reports_truncated = true;
        if (covered) return;
        self.summary.observed_roots += 1;
        const single_root = self.summary.observed_roots == 1;
        self.summary.aggregate_owner_identity = if (single_root) @intFromPtr(entry.owner) else null;
        self.summary.requested_live_bytes = if (single_root) after.requested_live_bytes else null;
        self.summary.requested_owner_interval_peak_bytes = if (single_root and finished != null) finished.?.peak else null;
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
    try std.testing.expectEqual(@as(u64, 31), measured.requested_owner_interval_peak_bytes.?);
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
    try std.testing.expect(measured.requested_owner_interval_peak_bytes == null);
    try std.testing.expectEqual(@as(u8, 2), measured.reports_count);
    try std.testing.expectEqual(@as(u64, 11), measured.reports[0].usage.requested_live_bytes);
    try std.testing.expectEqual(@as(u64, 17), measured.reports[1].usage.requested_live_bytes);
    try std.testing.expectEqual(@as(u64, 17), measured.maximum_individual_lifetime_peak_bytes.?);
}

test "registry inline interval allocates no metadata and preserves counters with exact peak" {
    const a = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(a, .{});
    const owner = try observer.State.createWithMetadata(a, failing.allocator(), .from_owner_creation);
    defer owner.release();
    const allocator = owner.allocator();
    // Reserve payload bookkeeping, then reject any new owner-metadata request.
    const first = try allocator.alloc(u8, 11);
    allocator.free(first);
    failing.fail_index = failing.alloc_index;
    var registry: Registry = undefined;
    registry.begin(a);
    bind(.model, owner);
    const bytes = try allocator.alloc(u8, 7);
    allocator.free(bytes);
    const measured = registry.finish();
    try std.testing.expectEqual(@as(usize, 0), measured.metadata_failures);
    try std.testing.expectEqual(@as(u64, 1), measured.allocations.?);
    try std.testing.expectEqual(@as(u64, 1), measured.frees.?);
    try std.testing.expectEqual(@as(u64, 7), measured.requested_owner_interval_peak_bytes.?);
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(@as(u64, 11), measured.maximum_individual_lifetime_peak_bytes.?);
}

test "owned report copies actual owner interval peak after registry and allocator retirement" {
    const a = std.testing.allocator;
    const owner = try observer.State.createWithMetadata(a, a, .from_owner_creation);
    const allocator = owner.allocator();
    const historical = try allocator.alloc(u8, 3072);
    allocator.free(historical);
    var registry: Registry = undefined;
    registry.begin(a);
    bind(.model, owner);
    const bytes = try allocator.alloc(u8, 61);
    allocator.free(bytes);
    owner.release();
    const summary = registry.finish();
    const snapshot = @import("diagnostics.zig").Snapshot{ .runtime = .{ .allocation_owners = summary } };
    const report = @import("invocation_report.zig").Report.init(snapshot);
    try std.testing.expectEqual(@as(u64, 61), report.observed_host_owner_interval_peak_bytes.?);
    try std.testing.expectEqual(@as(u64, 3072), report.maximum_individual_host_owner_lifetime_peak_bytes.?);
    try std.testing.expectEqual(@as(u64, 0), report.observed_host_requested_live_bytes.?);
    try std.testing.expect(!report.allocator_coverage_incomplete);
    try std.testing.expect(report.whole_backend_residency_and_invocation_peak == .unavailable);
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

test "nested owner interval subscriptions survive registry overflow reallocations and caller retirement" {
    const a = std.testing.allocator;
    var outer: Registry = undefined;
    outer.begin(a);
    var inner: Registry = undefined;
    inner.begin(a);
    for (0..33) |index| {
        const owner = try observer.State.createWithMetadata(a, a, .from_owner_creation);
        bind(.compute, owner);
        const payload = try owner.allocator().alloc(u8, index + 1);
        owner.allocator().free(payload);
        owner.release();
    }
    const child = inner.finish();
    const parent = outer.finish();
    try std.testing.expectEqual(@as(usize, 33), child.owners);
    try std.testing.expectEqual(@as(usize, 33), parent.owners);
    try std.testing.expectEqual(@as(u64, 33), child.maximum_individual_lifetime_peak_bytes.?);
    try std.testing.expectEqual(child.maximum_individual_lifetime_peak_bytes, parent.maximum_individual_lifetime_peak_bytes);
    try std.testing.expect(child.reports_truncated and parent.reports_truncated);
    try std.testing.expect(child.requested_owner_interval_peak_bytes == null and parent.requested_owner_interval_peak_bytes == null);
    try std.testing.expectEqual(@as(usize, 0), child.metadata_failures);
    try std.testing.expectEqual(@as(usize, 0), parent.metadata_failures);
}

test "registry common eight subscriptions use stable inline nodes with no metadata allocation" {
    const a = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 8 });
    var owners: [8]*observer.State = undefined;
    var count: usize = 0;
    defer for (owners[0..count]) |owner| owner.release();
    for (&owners) |*owner| {
        owner.* = try observer.State.createWithMetadata(a, failing.allocator(), .from_owner_creation);
        count += 1;
    }
    var registry: Registry = undefined;
    registry.begin(failing.allocator());
    defer if (registry.attached) {
        _ = registry.finish();
    };
    for (owners, 0..) |owner, index| {
        bind(.compute, owner);
        try std.testing.expect(registry.inline_entries[index].interval == &registry.inline_intervals[index]);
        try std.testing.expect(!registry.inline_intervals[index].heap_owned);
    }
    const observed = registry.finish();
    try std.testing.expectEqual(@as(usize, 8), observed.owners);
    try std.testing.expectEqual(@as(usize, 0), observed.metadata_failures);
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 8), failing.alloc_index);
    for (observed.reports[0..observed.reports_count]) |report| try std.testing.expectEqual(@as(u64, 0), report.requested_owner_interval_peak_bytes.?);
    for (owners) |owner| {
        try std.testing.expect(owner.intervals == null);
        try std.testing.expectEqual(@as(usize, 1), owner.references.load(.acquire));
    }
}

test "registry overflow interval OOM preserves missing peak coverage and releases durable entry ownership" {
    const a = std.testing.allocator;
    var owners: [8]*observer.State = undefined;
    var count: usize = 0;
    defer for (owners[0..count]) |owner| owner.release();
    for (&owners) |*owner| {
        owner.* = try observer.State.createWithMetadata(a, a, .from_owner_creation);
        count += 1;
    }
    var failing = std.testing.FailingAllocator.init(a, .{});
    const overflow_owner = try observer.State.createWithMetadata(a, failing.allocator(), .from_owner_creation);
    defer overflow_owner.release();
    const allocator = overflow_owner.allocator();
    const warm = try allocator.alloc(u8, 11);
    allocator.free(warm);
    failing.fail_index = failing.alloc_index;
    var registry: Registry = undefined;
    registry.begin(a);
    defer if (registry.attached) {
        _ = registry.finish();
    };
    for (owners) |owner| bind(.compute, owner);
    bind(.model, overflow_owner);
    try std.testing.expectEqual(@as(usize, 1), registry.overflow.items.len);
    try std.testing.expect(registry.overflow.items[0].interval == null);
    try std.testing.expect(overflow_owner.intervals == null);
    try std.testing.expectEqual(@as(usize, 2), overflow_owner.references.load(.acquire));
    const payload = try allocator.alloc(u8, 7);
    allocator.free(payload);
    const measured = registry.finish();
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 9), measured.owners);
    try std.testing.expectEqual(@as(usize, 1), measured.metadata_failures);
    try std.testing.expect(measured.reports_truncated);
    try std.testing.expect(measured.requested_owner_interval_peak_bytes == null);
    try std.testing.expectEqual(@as(usize, 1), overflow_owner.references.load(.acquire));
    const usage = overflow_owner.snapshot();
    try std.testing.expectEqual(@as(u64, 2), usage.allocations);
    try std.testing.expectEqual(@as(u64, 2), usage.frees);
    try std.testing.expectEqual(@as(u64, 0), usage.requested_live_bytes);
    try std.testing.expectEqual(@as(u64, 11), usage.requested_lifetime_peak_bytes);
}
