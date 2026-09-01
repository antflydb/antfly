// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const platform_clock = @import("antfly_platform").clock;
const platform_time = @import("antfly_platform").time;
const metadata_api = @import("api.zig");
const metadata_table_manager = @import("table_manager.zig");
const metadata_storage = @import("storage/raft_apply_store.zig");

pub const CatalogProjectionReader = struct {
    mutex: std.Io.Mutex = .init,
    cache: Cache = .{},
    build_flight: ?*BuildFlight = null,
    cache_hits: std.atomic.Value(u64) = .init(0),
    refresh_builds: std.atomic.Value(u64) = .init(0),
    coalesced_waits: std.atomic.Value(u64) = .init(0),
    deadline_timeouts: std.atomic.Value(u64) = .init(0),
    unstable_captures: std.atomic.Value(u64) = .init(0),

    pub const Diagnostics = struct {
        cache_hits: u64 = 0,
        refresh_builds: u64 = 0,
        coalesced_waits: u64 = 0,
        deadline_timeouts: u64 = 0,
        unstable_captures: u64 = 0,
    };

    pub const Source = struct {
        ptr: *anyopaque,
        vtable: *const VTable,

        pub const VTable = struct {
            ensure_listener_registered: *const fn (ptr: *anyopaque) anyerror!void,
            catalog_epoch: *const fn (ptr: *anyopaque) u64,
            capture_projection: *const fn (
                ptr: *anyopaque,
                alloc: std.mem.Allocator,
                metadata_group_id: u64,
                deadline_ns: ?u64,
            ) anyerror!metadata_storage.CatalogProjectionSnapshot,
        };

        fn ensureListenerRegistered(self: Source) !void {
            try self.vtable.ensure_listener_registered(self.ptr);
        }

        fn catalogEpoch(self: Source) u64 {
            return self.vtable.catalog_epoch(self.ptr);
        }

        fn captureProjection(
            self: Source,
            alloc: std.mem.Allocator,
            metadata_group_id: u64,
            deadline_ns: ?u64,
        ) !metadata_storage.CatalogProjectionSnapshot {
            return try self.vtable.capture_projection(self.ptr, alloc, metadata_group_id, deadline_ns);
        }
    };

    pub const Snapshot = struct {
        const RangeSpan = struct { start: usize, len: usize };

        metadata_incarnation: ?metadata_api.MetadataClusterIncarnation = null,
        catalog_revision: u64 = 0,
        tables: []metadata_table_manager.TableRecord = &.{},
        ranges: []metadata_table_manager.RangeRecord = &.{},
        index: metadata_api.CatalogProjectionIndex = .{},
        table_name_indexes: std.StringHashMapUnmanaged(usize) = .empty,
        table_range_spans: std.AutoHashMapUnmanaged(u64, RangeSpan) = .empty,

        pub fn deinit(self: *Snapshot, alloc: std.mem.Allocator) void {
            self.table_name_indexes.deinit(alloc);
            self.table_range_spans.deinit(alloc);
            self.index.deinit(alloc);
            freeTables(alloc, self.tables);
            freeRanges(alloc, self.ranges);
            self.* = .{};
        }
    };

    /// Immutable projection ownership shared by the cache and active readers.
    /// Retaining under the reader mutex closes the load/retire race; cloning
    /// then proceeds without holding the publication critical section.
    const SharedSnapshot = struct {
        alloc: std.mem.Allocator,
        refs: std.atomic.Value(usize) = .init(1),
        value: Snapshot,

        fn create(alloc: std.mem.Allocator, value: Snapshot) !*SharedSnapshot {
            const shared = try alloc.create(SharedSnapshot);
            shared.* = .{ .alloc = alloc, .value = value };
            return shared;
        }

        fn retain(self: *SharedSnapshot) void {
            const previous = self.refs.fetchAdd(1, .acq_rel);
            std.debug.assert(previous != 0 and previous != std.math.maxInt(usize));
        }

        fn release(self: *SharedSnapshot) void {
            const previous = self.refs.fetchSub(1, .acq_rel);
            std.debug.assert(previous != 0);
            if (previous != 1) return;
            var value = self.value;
            value.deinit(self.alloc);
            self.alloc.destroy(self);
        }
    };

    const SnapshotLease = struct {
        shared: *SharedSnapshot,

        fn snapshot(self: @This()) *const Snapshot {
            return &self.shared.value;
        }

        fn deinit(self: *@This()) void {
            self.shared.release();
            self.* = undefined;
        }
    };

    const BuildFlight = struct {
        alloc: std.mem.Allocator,
        refs: std.atomic.Value(usize) = .init(1),
        ready: std.Io.Event = .unset,
        failure: ?anyerror = null,

        fn retain(self: *BuildFlight) void {
            const previous = self.refs.fetchAdd(1, .acq_rel);
            std.debug.assert(previous != 0 and previous != std.math.maxInt(usize));
        }

        fn release(self: *BuildFlight) void {
            const previous = self.refs.fetchSub(1, .acq_rel);
            std.debug.assert(previous != 0);
            if (previous == 1) self.alloc.destroy(self);
        }
    };

    const Cache = struct {
        catalog_epoch: u64 = 0,
        reusable: bool = false,
        snapshot: ?*SharedSnapshot = null,

        fn deinit(self: *Cache) void {
            if (self.snapshot) |snapshot| snapshot.release();
            self.* = .{};
        }
    };

    pub fn deinit(self: *CatalogProjectionReader, alloc: std.mem.Allocator) void {
        _ = alloc;
        std.debug.assert(self.build_flight == null);
        self.cache.deinit();
        self.* = .{};
    }

    pub fn diagnostics(self: *const CatalogProjectionReader) Diagnostics {
        return .{
            .cache_hits = self.cache_hits.load(.monotonic),
            .refresh_builds = self.refresh_builds.load(.monotonic),
            .coalesced_waits = self.coalesced_waits.load(.monotonic),
            .deadline_timeouts = self.deadline_timeouts.load(.monotonic),
            .unstable_captures = self.unstable_captures.load(.monotonic),
        };
    }

    pub fn lock(self: *CatalogProjectionReader) void {
        self.mutex.lockUncancelable(std.Options.debug_io);
    }

    pub fn unlock(self: *CatalogProjectionReader) void {
        self.mutex.unlock(std.Options.debug_io);
    }

    pub fn lockUntil(self: *CatalogProjectionReader, deadline_ns: ?u64) bool {
        const deadline = deadline_ns orelse {
            self.lock();
            return true;
        };
        while (true) {
            if (platform_time.monotonicNs() >= deadline) return false;
            if (self.mutex.tryLock()) return true;
            platform_clock.Clock.real().sleepMs(1);
        }
    }

    pub fn validationSnapshotLocked(
        self: *CatalogProjectionReader,
        alloc: std.mem.Allocator,
        metadata_group_id: u64,
        source: Source,
        deadline_ns: ?u64,
    ) !*const Snapshot {
        try ensureBeforeDeadline(deadline_ns);
        try source.ensureListenerRegistered();
        try ensureBeforeDeadline(deadline_ns);

        const current_epoch = source.catalogEpoch();
        if (self.cache.reusable and self.cache.snapshot != null and self.cache.catalog_epoch == current_epoch) {
            return &(self.cache.snapshot orelse unreachable).value;
        }

        // The apply store captures both namespaces from one point-in-time read
        // transaction, so concurrent publication does not make the captured
        // value internally inconsistent. If the listener epoch changes while
        // cloning, return this coherent point-in-time value to the current
        // caller but mark it non-reusable; the next caller refreshes instead
        // of turning ordinary catalog churn into an internal error.
        try ensureBeforeDeadline(deadline_ns);
        const before = source.catalogEpoch();
        var fresh = try capture(alloc, metadata_group_id, source, deadline_ns);
        errdefer fresh.deinit(alloc);
        const after = source.catalogEpoch();
        try ensureBeforeDeadline(deadline_ns);
        if (self.cache.snapshot) |cached| {
            if (std.meta.eql(cached.value.metadata_incarnation, fresh.metadata_incarnation) and
                fresh.catalog_revision < cached.value.catalog_revision)
            {
                return error.CatalogProjectionRevisionRegressed;
            }
        }
        const shared = try SharedSnapshot.create(alloc, fresh);
        fresh = .{};
        if (self.cache.snapshot) |snapshot| snapshot.release();
        self.cache = .{
            .catalog_epoch = after,
            .reusable = before == after,
            .snapshot = shared,
        };
        return &shared.value;
    }

    /// Retain a coherent projection while doing all allocation-heavy cloning
    /// outside the publication mutex. Cache misses are single-flight: one
    /// caller captures and indexes while peers wait on a deadline-aware event.
    fn snapshotLease(
        self: *CatalogProjectionReader,
        alloc: std.mem.Allocator,
        metadata_group_id: u64,
        source: Source,
        deadline_ns: ?u64,
    ) !SnapshotLease {
        try ensureBeforeDeadline(deadline_ns);
        try source.ensureListenerRegistered();
        try ensureBeforeDeadline(deadline_ns);

        while (true) {
            const current_epoch = source.catalogEpoch();
            if (!self.lockUntil(deadline_ns)) return error.CatalogRoutingSnapshotTimeout;
            if (self.cache.reusable and self.cache.catalog_epoch == current_epoch) {
                if (self.cache.snapshot) |snapshot| {
                    snapshot.retain();
                    _ = self.cache_hits.fetchAdd(1, .monotonic);
                    self.unlock();
                    return .{ .shared = snapshot };
                }
            }
            if (self.build_flight) |flight| {
                flight.retain();
                _ = self.coalesced_waits.fetchAdd(1, .monotonic);
                self.unlock();
                waitForBuildFlight(flight, deadline_ns) catch |err| {
                    if (err == error.CatalogRoutingSnapshotTimeout) _ = self.deadline_timeouts.fetchAdd(1, .monotonic);
                    flight.release();
                    return err;
                };
                const failure = flight.failure;
                flight.release();
                if (failure) |err| switch (err) {
                    // A coalesced builder may have had a shorter deadline than
                    // this waiter. Retry under the waiter's own budget instead
                    // of leaking another request's timeout semantics.
                    error.CatalogRoutingSnapshotTimeout => {
                        try ensureBeforeDeadline(deadline_ns);
                        continue;
                    },
                    else => return err,
                };
                continue;
            }

            const flight = alloc.create(BuildFlight) catch |err| {
                self.unlock();
                return err;
            };
            flight.* = .{ .alloc = alloc };
            self.build_flight = flight;
            _ = self.refresh_builds.fetchAdd(1, .monotonic);
            self.unlock();
            return try self.buildSnapshotLease(alloc, metadata_group_id, source, deadline_ns, flight);
        }
    }

    fn buildSnapshotLease(
        self: *CatalogProjectionReader,
        alloc: std.mem.Allocator,
        metadata_group_id: u64,
        source: Source,
        deadline_ns: ?u64,
        flight: *BuildFlight,
    ) !SnapshotLease {
        const before = source.catalogEpoch();
        var fresh = capture(alloc, metadata_group_id, source, deadline_ns) catch |err| {
            if (err == error.CatalogRoutingSnapshotTimeout) _ = self.deadline_timeouts.fetchAdd(1, .monotonic);
            self.finishBuildFlight(flight, err);
            return err;
        };
        errdefer fresh.deinit(alloc);
        const after = source.catalogEpoch();
        ensureBeforeDeadline(deadline_ns) catch |err| {
            if (err == error.CatalogRoutingSnapshotTimeout) _ = self.deadline_timeouts.fetchAdd(1, .monotonic);
            self.finishBuildFlight(flight, err);
            return err;
        };
        const shared = SharedSnapshot.create(alloc, fresh) catch |err| {
            self.finishBuildFlight(flight, err);
            return err;
        };
        fresh = .{};

        self.lock();
        var old: ?*SharedSnapshot = null;
        var failure: ?anyerror = null;
        if (deadlineExpired(deadline_ns)) {
            failure = error.CatalogRoutingSnapshotTimeout;
            _ = self.deadline_timeouts.fetchAdd(1, .monotonic);
        } else if (self.cache.snapshot) |cached| {
            if (std.meta.eql(cached.value.metadata_incarnation, shared.value.metadata_incarnation) and
                shared.value.catalog_revision < cached.value.catalog_revision)
            {
                failure = error.CatalogProjectionRevisionRegressed;
            }
        }
        if (failure == null and before == after) {
            shared.retain(); // Cache ownership; the original reference is the caller's lease.
            old = self.cache.snapshot;
            self.cache = .{
                .catalog_epoch = after,
                .reusable = true,
                .snapshot = shared,
            };
        } else if (failure == null) {
            _ = self.unstable_captures.fetchAdd(1, .monotonic);
        }
        std.debug.assert(self.build_flight == flight);
        self.build_flight = null;
        flight.failure = failure;
        flight.ready.set(std.Options.debug_io);
        self.unlock();

        if (old) |snapshot| snapshot.release();
        flight.release();
        if (failure) |err| {
            shared.release();
            return err;
        }
        return .{ .shared = shared };
    }

    fn finishBuildFlight(self: *CatalogProjectionReader, flight: *BuildFlight, failure: anyerror) void {
        self.lock();
        std.debug.assert(self.build_flight == flight);
        self.build_flight = null;
        flight.failure = failure;
        flight.ready.set(std.Options.debug_io);
        self.unlock();
        flight.release();
    }

    pub fn routingSnapshot(
        self: *CatalogProjectionReader,
        alloc: std.mem.Allocator,
        metadata_group_id: u64,
        source: Source,
        deadline_ns: ?u64,
    ) !metadata_api.CatalogRoutingSnapshot {
        var lease = try self.snapshotLease(alloc, metadata_group_id, source, deadline_ns);
        defer lease.deinit();
        const snapshot = lease.snapshot();
        const tables = try cloneTables(alloc, snapshot.tables, deadline_ns);
        errdefer freeTables(alloc, tables);
        return .{
            .metadata_group_id = metadata_group_id,
            .metadata_incarnation = snapshot.metadata_incarnation,
            .catalog_revision = snapshot.catalog_revision,
            .change_token = .{
                .metadata_group_id = metadata_group_id,
                .metadata_incarnation = snapshot.metadata_incarnation,
                .revision = snapshot.catalog_revision,
            },
            .tables = tables,
            .ranges = try cloneRanges(alloc, snapshot.ranges, deadline_ns),
        };
    }

    /// Clone only one table and its ranges from the shared immutable cache.
    /// Indexed lookup is constant-time and allocation/copy cost scales with
    /// the selected table rather than total tenant count.
    pub fn tableRoutingSnapshot(
        self: *CatalogProjectionReader,
        alloc: std.mem.Allocator,
        metadata_group_id: u64,
        source: Source,
        table_name: []const u8,
        deadline_ns: ?u64,
    ) !metadata_api.CatalogRoutingSnapshot {
        var lease = try self.snapshotLease(alloc, metadata_group_id, source, deadline_ns);
        defer lease.deinit();
        const snapshot = lease.snapshot();
        const table_index = snapshot.table_name_indexes.get(table_name) orelse return .{
            .metadata_group_id = metadata_group_id,
            .metadata_incarnation = snapshot.metadata_incarnation,
            .catalog_revision = snapshot.catalog_revision,
            .change_token = .{
                .metadata_group_id = metadata_group_id,
                .metadata_incarnation = snapshot.metadata_incarnation,
                .revision = snapshot.catalog_revision,
            },
            .tables = &.{},
            .ranges = &.{},
        };
        const table = snapshot.tables[table_index];

        const tables = try alloc.alloc(metadata_table_manager.TableRecord, 1);
        errdefer alloc.free(tables);
        // The point projection is also the physical-owner descriptor source.
        // Unlike the catalog-wide routing snapshot, its cost is bounded to one
        // table, so retain the complete schema/index definition. Blanking
        // these fields here caused production data-Raft entries to persist an
        // empty storage-owner configuration while test adapters (which cloned
        // full admin records) continued to pass.
        tables[0] = try metadata_table_manager.cloneTable(alloc, table);
        errdefer metadata_table_manager.freeTable(alloc, tables[0]);
        const span = snapshot.table_range_spans.get(table.table_id) orelse Snapshot.RangeSpan{ .start = 0, .len = 0 };
        const table_ranges = snapshot.ranges[span.start..][0..span.len];
        const ranges = try alloc.alloc(metadata_table_manager.RangeRecord, table_ranges.len);
        var cloned: usize = 0;
        errdefer {
            for (ranges[0..cloned]) |range| metadata_table_manager.freeRange(alloc, range);
            alloc.free(ranges);
        }
        for (table_ranges) |range| {
            try ensureBeforeDeadline(deadline_ns);
            ranges[cloned] = try metadata_table_manager.cloneRoutingRange(alloc, range);
            cloned += 1;
        }
        try ensureBeforeDeadline(deadline_ns);
        return .{
            .metadata_group_id = metadata_group_id,
            .metadata_incarnation = snapshot.metadata_incarnation,
            .catalog_revision = snapshot.catalog_revision,
            .change_token = .{
                .metadata_group_id = metadata_group_id,
                .metadata_incarnation = snapshot.metadata_incarnation,
                .revision = snapshot.catalog_revision,
            },
            .tables = tables,
            .ranges = ranges,
        };
    }

    /// Validate a publication contract against an immutable retained
    /// projection. Linearizability remains the caller's responsibility; this
    /// method ensures the potentially expensive refresh never holds the cache
    /// publication mutex.
    pub fn matchesPublication(
        self: *CatalogProjectionReader,
        alloc: std.mem.Allocator,
        metadata_group_id: u64,
        source: Source,
        incarnation: ?metadata_api.MetadataClusterIncarnation,
        contract: metadata_api.CatalogPublicationContract,
    ) !bool {
        var lease = try self.snapshotLease(alloc, metadata_group_id, source, null);
        defer lease.deinit();
        const snapshot = lease.snapshot();
        return snapshot.index.matchesPublication(
            contract,
            metadata_group_id,
            incarnation,
            snapshot.tables,
            snapshot.ranges,
        );
    }

    pub fn matchesTablePublication(
        self: *CatalogProjectionReader,
        alloc: std.mem.Allocator,
        metadata_group_id: u64,
        source: Source,
        incarnation: ?metadata_api.MetadataClusterIncarnation,
        contract: metadata_api.CatalogTablePublicationContract,
    ) !bool {
        var lease = try self.snapshotLease(alloc, metadata_group_id, source, null);
        defer lease.deinit();
        const snapshot = lease.snapshot();
        return snapshot.index.matchesTablePublication(
            contract,
            metadata_group_id,
            incarnation,
            snapshot.tables,
        );
    }

    pub fn freeRoutingSnapshot(
        _: *CatalogProjectionReader,
        alloc: std.mem.Allocator,
        snapshot: *metadata_api.CatalogRoutingSnapshot,
    ) void {
        freeTables(alloc, snapshot.tables);
        freeRanges(alloc, snapshot.ranges);
        snapshot.* = undefined;
    }

    pub fn cachedSnapshotLocked(self: *const CatalogProjectionReader) ?*const Snapshot {
        if (self.cache.snapshot) |snapshot| return &snapshot.value;
        return null;
    }

    pub fn cachedEpochLocked(self: *const CatalogProjectionReader) u64 {
        return self.cache.catalog_epoch;
    }

    fn capture(
        alloc: std.mem.Allocator,
        metadata_group_id: u64,
        source: Source,
        deadline_ns: ?u64,
    ) !Snapshot {
        try ensureBeforeDeadline(deadline_ns);
        var snapshot: Snapshot = .{};
        errdefer snapshot.deinit(alloc);
        const projected = try source.captureProjection(alloc, metadata_group_id, deadline_ns);
        snapshot.metadata_incarnation = projected.metadata_incarnation;
        snapshot.catalog_revision = projected.catalog_revision;
        snapshot.tables = projected.tables;
        snapshot.ranges = projected.ranges;
        try ensureBeforeDeadline(deadline_ns);
        std.sort.pdq(metadata_table_manager.RangeRecord, snapshot.ranges, {}, rangeLessThan);
        // std.sort is not fallible, so stop immediately afterward rather than
        // performing another complete indexing pass for an expired caller.
        try ensureBeforeDeadline(deadline_ns);
        snapshot.index = try metadata_api.CatalogProjectionIndex.initUntil(
            alloc,
            snapshot.tables,
            snapshot.ranges,
            deadline_ns,
        );
        try snapshot.table_name_indexes.ensureTotalCapacity(alloc, @intCast(snapshot.tables.len));
        try snapshot.table_range_spans.ensureTotalCapacity(alloc, @intCast(snapshot.tables.len));
        for (snapshot.tables, 0..) |table, index| {
            if (index % 64 == 0) try ensureBeforeDeadline(deadline_ns);
            if (snapshot.table_name_indexes.contains(table.name)) return error.InvalidCatalogProjection;
            snapshot.table_name_indexes.putAssumeCapacity(table.name, index);
        }
        var first: usize = 0;
        while (first < snapshot.ranges.len) {
            if (first % 64 == 0) try ensureBeforeDeadline(deadline_ns);
            var end = first + 1;
            while (end < snapshot.ranges.len and snapshot.ranges[end].table_id == snapshot.ranges[first].table_id) : (end += 1) {
                if (end % 64 == 0) try ensureBeforeDeadline(deadline_ns);
            }
            const table_id = snapshot.ranges[first].table_id;
            if (!snapshot.index.table_indexes.contains(table_id)) return error.InvalidCatalogProjection;
            snapshot.table_range_spans.putAssumeCapacity(table_id, .{ .start = first, .len = end - first });
            first = end;
        }
        try ensureBeforeDeadline(deadline_ns);
        return snapshot;
    }
};

fn rangeLessThan(_: void, a: metadata_table_manager.RangeRecord, b: metadata_table_manager.RangeRecord) bool {
    if (a.table_id != b.table_id) return a.table_id < b.table_id;
    return switch (std.mem.order(u8, a.start_key, b.start_key)) {
        .lt => true,
        .gt => false,
        .eq => a.group_id < b.group_id,
    };
}

fn ensureBeforeDeadline(deadline_ns: ?u64) !void {
    if (deadlineExpired(deadline_ns)) return error.CatalogRoutingSnapshotTimeout;
}

fn deadlineExpired(deadline_ns: ?u64) bool {
    return if (deadline_ns) |deadline| platform_time.monotonicNs() >= deadline else false;
}

fn waitForBuildFlight(flight: *CatalogProjectionReader.BuildFlight, deadline_ns: ?u64) !void {
    const deadline = deadline_ns orelse {
        flight.ready.waitUncancelable(std.Options.debug_io);
        return;
    };
    while (!flight.ready.isSet()) {
        const now = platform_time.monotonicNs();
        if (now >= deadline) return error.CatalogRoutingSnapshotTimeout;
        flight.ready.waitTimeout(std.Options.debug_io, .{
            .duration = .{
                .raw = std.Io.Duration.fromNanoseconds(@intCast(deadline - now)),
                .clock = .awake,
            },
        }) catch |err| switch (err) {
            error.Timeout => continue,
            error.Canceled => return error.CatalogRoutingSnapshotTimeout,
        };
    }
}

fn cloneTables(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.TableRecord,
    deadline_ns: ?u64,
) ![]metadata_table_manager.TableRecord {
    try ensureBeforeDeadline(deadline_ns);
    const out = try alloc.alloc(metadata_table_manager.TableRecord, records.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |record| metadata_table_manager.freeTable(alloc, record);
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        try ensureBeforeDeadline(deadline_ns);
        out[i] = try metadata_table_manager.cloneRoutingTable(alloc, record);
        cloned = i + 1;
    }
    try ensureBeforeDeadline(deadline_ns);
    return out;
}

fn cloneRanges(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.RangeRecord,
    deadline_ns: ?u64,
) ![]metadata_table_manager.RangeRecord {
    try ensureBeforeDeadline(deadline_ns);
    const out = try alloc.alloc(metadata_table_manager.RangeRecord, records.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        try ensureBeforeDeadline(deadline_ns);
        out[i] = try metadata_table_manager.cloneRoutingRange(alloc, record);
        cloned = i + 1;
    }
    try ensureBeforeDeadline(deadline_ns);
    return out;
}

fn freeTables(alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
    for (records) |record| metadata_table_manager.freeTable(alloc, record);
    if (records.len > 0) alloc.free(records);
}

fn freeRanges(alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
    for (records) |record| metadata_table_manager.freeRange(alloc, record);
    if (records.len > 0) alloc.free(records);
}

test "catalog projection churn returns coherent snapshots and only caches stable captures" {
    const FakeSource = struct {
        epoch: u64 = 0,
        revision: u64 = 0,
        captures: usize = 0,
        advance_epoch_during_capture: bool = true,

        fn source(self: *@This()) CatalogProjectionReader.Source {
            return .{ .ptr = self, .vtable = &.{
                .ensure_listener_registered = ensureListenerRegistered,
                .catalog_epoch = catalogEpoch,
                .capture_projection = captureProjection,
            } };
        }

        fn ensureListenerRegistered(_: *anyopaque) !void {}

        fn catalogEpoch(ptr: *anyopaque) u64 {
            return cast(ptr).epoch;
        }

        fn captureProjection(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            deadline_ns: ?u64,
        ) !metadata_storage.CatalogProjectionSnapshot {
            try ensureBeforeDeadline(deadline_ns);
            const self = cast(ptr);
            self.captures += 1;
            self.revision += 1;
            if (self.advance_epoch_during_capture) self.epoch += 1;
            return .{
                .metadata_incarnation = null,
                .catalog_revision = self.revision,
                .tables = &.{},
                .ranges = &.{},
            };
        }

        fn cast(ptr: *anyopaque) *@This() {
            return @ptrCast(@alignCast(ptr));
        }
    };

    var fake = FakeSource{};
    var reader: CatalogProjectionReader = .{};
    defer reader.deinit(std.testing.allocator);

    var first = try reader.routingSnapshot(std.testing.allocator, 91, fake.source(), null);
    reader.freeRoutingSnapshot(std.testing.allocator, &first);
    try std.testing.expectEqual(@as(usize, 1), fake.captures);
    try std.testing.expect(!reader.cache.reusable);

    var second = try reader.routingSnapshot(std.testing.allocator, 91, fake.source(), null);
    reader.freeRoutingSnapshot(std.testing.allocator, &second);
    try std.testing.expectEqual(@as(usize, 2), fake.captures);
    try std.testing.expect(!reader.cache.reusable);

    fake.advance_epoch_during_capture = false;
    var stable = try reader.routingSnapshot(std.testing.allocator, 91, fake.source(), null);
    reader.freeRoutingSnapshot(std.testing.allocator, &stable);
    try std.testing.expectEqual(@as(usize, 3), fake.captures);
    try std.testing.expect(reader.cache.reusable);

    var cached = try reader.routingSnapshot(std.testing.allocator, 91, fake.source(), null);
    reader.freeRoutingSnapshot(std.testing.allocator, &cached);
    try std.testing.expectEqual(@as(usize, 3), fake.captures);
}

test "catalog projection cache rejects revision regression within one authority" {
    const FakeSource = struct {
        epoch: u64 = 1,
        revision: u64 = 7,

        fn source(self: *@This()) CatalogProjectionReader.Source {
            return .{ .ptr = self, .vtable = &.{
                .ensure_listener_registered = ensureListenerRegistered,
                .catalog_epoch = catalogEpoch,
                .capture_projection = captureProjection,
            } };
        }

        fn ensureListenerRegistered(_: *anyopaque) !void {}

        fn catalogEpoch(ptr: *anyopaque) u64 {
            return cast(ptr).epoch;
        }

        fn captureProjection(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: ?u64,
        ) !metadata_storage.CatalogProjectionSnapshot {
            const self = cast(ptr);
            return .{
                .metadata_incarnation = null,
                .catalog_revision = self.revision,
                .tables = &.{},
                .ranges = &.{},
            };
        }

        fn cast(ptr: *anyopaque) *@This() {
            return @ptrCast(@alignCast(ptr));
        }
    };

    var fake = FakeSource{};
    var reader: CatalogProjectionReader = .{};
    defer reader.deinit(std.testing.allocator);

    var initial = try reader.routingSnapshot(std.testing.allocator, 91, fake.source(), null);
    reader.freeRoutingSnapshot(std.testing.allocator, &initial);
    fake.epoch += 1;
    fake.revision -= 1;
    try std.testing.expectError(
        error.CatalogProjectionRevisionRegressed,
        reader.routingSnapshot(std.testing.allocator, 91, fake.source(), null),
    );
    try std.testing.expectEqual(@as(u64, 7), reader.cache.snapshot.?.value.catalog_revision);
}

test "catalog projection cache coalesces concurrent refreshes outside its mutex" {
    const FakeSource = struct {
        captures: std.atomic.Value(u64) = .init(0),
        entered: std.Io.Event = .unset,
        proceed: std.Io.Event = .unset,

        fn source(self: *@This()) CatalogProjectionReader.Source {
            return .{ .ptr = self, .vtable = &.{
                .ensure_listener_registered = ensureListenerRegistered,
                .catalog_epoch = catalogEpoch,
                .capture_projection = captureProjection,
            } };
        }

        fn ensureListenerRegistered(_: *anyopaque) !void {}
        fn catalogEpoch(_: *anyopaque) u64 {
            return 1;
        }
        fn captureProjection(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: ?u64,
        ) !metadata_storage.CatalogProjectionSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.captures.fetchAdd(1, .acq_rel);
            self.entered.set(std.Options.debug_io);
            self.proceed.waitUncancelable(std.Options.debug_io);
            return .{ .metadata_incarnation = null, .catalog_revision = 1, .tables = &.{}, .ranges = &.{} };
        }
    };
    const Worker = struct {
        reader: *CatalogProjectionReader,
        source: CatalogProjectionReader.Source,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            var snapshot = self.reader.routingSnapshot(std.testing.allocator, 91, self.source, null) catch |err| {
                self.failure = err;
                return;
            };
            self.reader.freeRoutingSnapshot(std.testing.allocator, &snapshot);
        }
    };

    var fake: FakeSource = .{};
    var reader: CatalogProjectionReader = .{};
    defer reader.deinit(std.testing.allocator);
    var first = Worker{ .reader = &reader, .source = fake.source() };
    var second = Worker{ .reader = &reader, .source = fake.source() };
    var first_thread = try std.Thread.spawn(.{}, Worker.run, .{&first});
    fake.entered.waitUncancelable(std.Options.debug_io);
    var second_thread = try std.Thread.spawn(.{}, Worker.run, .{&second});

    const waiter_deadline = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    while (true) {
        reader.lock();
        const waiter_joined = if (reader.build_flight) |flight| flight.refs.load(.acquire) >= 2 else false;
        reader.unlock();
        if (waiter_joined) break;
        if (platform_time.monotonicNs() >= waiter_deadline) return error.TestUnexpectedResult;
        platform_clock.Clock.real().sleepMs(1);
    }
    fake.proceed.set(std.Options.debug_io);
    first_thread.join();
    second_thread.join();

    try std.testing.expect(first.failure == null);
    try std.testing.expect(second.failure == null);
    try std.testing.expectEqual(@as(u64, 1), fake.captures.load(.acquire));
    const diagnostics = reader.diagnostics();
    try std.testing.expectEqual(@as(u64, 1), diagnostics.refresh_builds);
    try std.testing.expectEqual(@as(u64, 1), diagnostics.coalesced_waits);
}

test "catalog projection waiter retries a shorter builder timeout" {
    const FakeSource = struct {
        captures: std.atomic.Value(u64) = .init(0),
        first_entered: std.Io.Event = .unset,
        release_first: std.Io.Event = .unset,

        fn source(self: *@This()) CatalogProjectionReader.Source {
            return .{ .ptr = self, .vtable = &.{
                .ensure_listener_registered = ensureListenerRegistered,
                .catalog_epoch = catalogEpoch,
                .capture_projection = captureProjection,
            } };
        }

        fn ensureListenerRegistered(_: *anyopaque) !void {}
        fn catalogEpoch(_: *anyopaque) u64 {
            return 1;
        }
        fn captureProjection(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: ?u64,
        ) !metadata_storage.CatalogProjectionSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const ordinal = self.captures.fetchAdd(1, .acq_rel);
            if (ordinal == 0) {
                self.first_entered.set(std.Options.debug_io);
                self.release_first.waitUncancelable(std.Options.debug_io);
            }
            return .{ .metadata_incarnation = null, .catalog_revision = 1, .tables = &.{}, .ranges = &.{} };
        }
    };
    const Worker = struct {
        reader: *CatalogProjectionReader,
        source: CatalogProjectionReader.Source,
        deadline_ns: u64,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            var snapshot = self.reader.routingSnapshot(std.testing.allocator, 91, self.source, self.deadline_ns) catch |err| {
                self.failure = err;
                return;
            };
            self.reader.freeRoutingSnapshot(std.testing.allocator, &snapshot);
        }
    };

    var fake: FakeSource = .{};
    var reader: CatalogProjectionReader = .{};
    defer reader.deinit(std.testing.allocator);
    var short = Worker{
        .reader = &reader,
        .source = fake.source(),
        .deadline_ns = platform_time.monotonicNs() + 20 * std.time.ns_per_ms,
    };
    var short_thread = try std.Thread.spawn(.{}, Worker.run, .{&short});
    fake.first_entered.waitUncancelable(std.Options.debug_io);
    var patient = Worker{
        .reader = &reader,
        .source = fake.source(),
        .deadline_ns = platform_time.monotonicNs() + 5 * std.time.ns_per_s,
    };
    var patient_thread = try std.Thread.spawn(.{}, Worker.run, .{&patient});

    const waiter_deadline = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    while (true) {
        reader.lock();
        const waiter_joined = if (reader.build_flight) |flight| flight.refs.load(.acquire) >= 2 else false;
        reader.unlock();
        if (waiter_joined) break;
        if (platform_time.monotonicNs() >= waiter_deadline) return error.TestUnexpectedResult;
        platform_clock.Clock.real().sleepMs(1);
    }
    platform_clock.Clock.real().sleepMs(25);
    fake.release_first.set(std.Options.debug_io);
    short_thread.join();
    patient_thread.join();

    try std.testing.expectEqualStrings("CatalogRoutingSnapshotTimeout", @errorName(short.failure.?));
    try std.testing.expect(patient.failure == null);
    try std.testing.expectEqual(@as(u64, 2), fake.captures.load(.acquire));
    const diagnostics = reader.diagnostics();
    try std.testing.expectEqual(@as(u64, 2), diagnostics.refresh_builds);
    try std.testing.expectEqual(@as(u64, 1), diagnostics.coalesced_waits);
}

test "catalog projection timeout does not publish a late build" {
    const FakeSource = struct {
        entered: std.Io.Event = .unset,
        proceed: std.Io.Event = .unset,

        fn source(self: *@This()) CatalogProjectionReader.Source {
            return .{ .ptr = self, .vtable = &.{
                .ensure_listener_registered = ensureListenerRegistered,
                .catalog_epoch = catalogEpoch,
                .capture_projection = captureProjection,
            } };
        }

        fn ensureListenerRegistered(_: *anyopaque) !void {}
        fn catalogEpoch(_: *anyopaque) u64 {
            return 1;
        }
        fn captureProjection(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: ?u64,
        ) !metadata_storage.CatalogProjectionSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.set(std.Options.debug_io);
            self.proceed.waitUncancelable(std.Options.debug_io);
            return .{ .metadata_incarnation = null, .catalog_revision = 1, .tables = &.{}, .ranges = &.{} };
        }
    };
    const Worker = struct {
        reader: *CatalogProjectionReader,
        source: CatalogProjectionReader.Source,
        deadline_ns: u64,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            var snapshot = self.reader.routingSnapshot(std.testing.allocator, 91, self.source, self.deadline_ns) catch |err| {
                self.failure = err;
                return;
            };
            self.reader.freeRoutingSnapshot(std.testing.allocator, &snapshot);
        }
    };

    var fake: FakeSource = .{};
    var reader: CatalogProjectionReader = .{};
    defer reader.deinit(std.testing.allocator);
    var worker = Worker{
        .reader = &reader,
        .source = fake.source(),
        .deadline_ns = platform_time.monotonicNs() + 10 * std.time.ns_per_ms,
    };
    var thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
    fake.entered.waitUncancelable(std.Options.debug_io);
    platform_clock.Clock.real().sleepMs(20);
    fake.proceed.set(std.Options.debug_io);
    thread.join();

    try std.testing.expect(worker.failure != null);
    try std.testing.expectEqualStrings("CatalogRoutingSnapshotTimeout", @errorName(worker.failure.?));
    reader.lock();
    defer reader.unlock();
    try std.testing.expect(reader.cachedSnapshotLocked() == null);
}
