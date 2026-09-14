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
const platform_sync = @import("antfly_platform").sync;
const Allocator = std.mem.Allocator;
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
const catalog_service = @import("../catalog/service.zig");
const work_lease = @import("work_lease.zig");
const maintenance_cancellation = @import("../maintenance_cancellation.zig");

pub const PublishRunStats = struct {
    published_namespaces: usize = 0,
    head_conflicts: usize = 0,
    idle_namespaces: usize = 0,
    lease_conflicts: usize = 0,
    lease_takeovers: usize = 0,
    budget_rejected_namespaces: usize = 0,
};

pub const BackgroundPublisher = struct {
    alloc: Allocator,
    io: std.Io,
    catalog: *catalog_service.CatalogService,
    poll_interval_ms: u64,
    lifecycle_mutex: std.Io.Mutex = .init,
    run_mutex: std.Io.Mutex = .init,
    budget_backoff: BudgetBackoff = .{},
    coalescing: PublicationCoalescing = .{},
    /// Thresholds are throughput targets, never an unbounded visibility gate.
    max_coalescing_delay_ms: u64 = 1000,
    scheduling_clock: SchedulingClock = .{},
    future: ?std.Io.Future(void) = null,
    stop_requested: std.atomic.Value(bool) = .init(false),
    stop_wake: std.Io.Event = .unset,
    idle_waiting: std.atomic.Value(bool) = .init(false),
    failure_mu: std.atomic.Mutex = .unlocked,
    run_failure: ?anyerror = null,
    lease_provider: ?work_lease.Provider = null,
    lease_owner_id: ?[]u8 = null,
    lease_ttl_ns: u64 = 30 * std.time.ns_per_s,

    pub fn init(alloc: Allocator, io: std.Io, catalog: *catalog_service.CatalogService, poll_interval_ms: u64) BackgroundPublisher {
        return initWithIo(alloc, io, catalog, poll_interval_ms);
    }

    pub fn initWithIo(
        alloc: Allocator,
        io: std.Io,
        catalog: *catalog_service.CatalogService,
        poll_interval_ms: u64,
    ) BackgroundPublisher {
        return .{
            .alloc = alloc,
            .io = io,
            .catalog = catalog,
            .poll_interval_ms = poll_interval_ms,
        };
    }

    pub fn deinit(self: *BackgroundPublisher) void {
        self.stop();
        self.budget_backoff.deinit(self.alloc);
        self.coalescing.deinit(self.alloc);
        if (self.lease_owner_id) |owner_id| self.alloc.free(owner_id);
        self.* = undefined;
    }

    pub fn start(self: *BackgroundPublisher) !void {
        self.lifecycle_mutex.lockUncancelable(self.io);
        defer self.lifecycle_mutex.unlock(self.io);
        if (self.future != null) return error.AlreadyStarted;
        self.stop_requested.store(false, .monotonic);
        self.idle_waiting.store(false, .monotonic);
        self.stop_wake.reset();
        lockAtomic(&self.failure_mu);
        self.run_failure = null;
        self.failure_mu.unlock();
        self.future = try self.io.concurrent(runLoop, .{self});
    }

    pub fn stop(self: *BackgroundPublisher) void {
        self.lifecycle_mutex.lockUncancelable(self.io);
        defer self.lifecycle_mutex.unlock(self.io);
        self.stop_requested.store(true, .monotonic);
        self.stop_wake.set(self.io);
        if (self.future) |*future| {
            // Wake the idle wait explicitly. Awaiting preserves cleanup in an
            // in-flight publication while avoiding the configured interval.
            _ = future.await(self.io);
            self.future = null;
        }
    }

    pub fn runtimeFailure(self: *BackgroundPublisher) ?anyerror {
        lockAtomic(&self.failure_mu);
        defer self.failure_mu.unlock();
        return self.run_failure;
    }

    pub fn configureLease(
        self: *BackgroundPublisher,
        provider: work_lease.Provider,
        owner_id: []const u8,
        ttl_ns: u64,
    ) !void {
        if (self.future != null) return error.AlreadyStarted;
        if (owner_id.len == 0) return error.InvalidLeaseOwner;
        if (ttl_ns == 0) return error.InvalidLeaseTtl;
        const owned_owner = try self.alloc.dupe(u8, owner_id);
        if (self.lease_owner_id) |current| self.alloc.free(current);
        self.lease_owner_id = owned_owner;
        self.lease_provider = provider;
        self.lease_ttl_ns = ttl_ns;
    }

    pub fn runOnce(self: *BackgroundPublisher) !PublishRunStats {
        return self.runOnceUntil(null);
    }

    pub fn runOnceUntil(
        self: *BackgroundPublisher,
        cancel_requested: ?*const std.atomic.Value(bool),
    ) !PublishRunStats {
        const cancellation = maintenance_cancellation.Token{
            .io = self.io,
            .requested = cancel_requested,
        };
        return self.runOnceWithToken(cancellation);
    }

    pub fn runOnceWithCancellation(self: *BackgroundPublisher, cancellation: CancellationToken) !PublishRunStats {
        return self.runOnceWithToken(.{ .io = self.io, .requested = &self.stop_requested, .cooperative = cancellation });
    }

    /// Both the standalone publisher and managed runtime honor pending-tail
    /// deadlines even when their normal maintenance poll is much longer.
    pub fn nextWakeDelayMs(self: *BackgroundPublisher, poll_ms: u64) u64 {
        self.run_mutex.lockUncancelable(self.io);
        defer self.run_mutex.unlock(self.io);
        return self.coalescing.nextWaitMs(self.scheduling_clock.now(self.io), @max(poll_ms, 1));
    }

    fn runOnceWithToken(self: *BackgroundPublisher, cancellation: maintenance_cancellation.Token) !PublishRunStats {
        try self.run_mutex.lock(self.io);
        defer self.run_mutex.unlock(self.io);
        const namespaces = try self.catalog.listNamespacesAlloc(self.alloc);
        defer self.catalog.freeNamespaces(self.alloc, namespaces);

        var stats = PublishRunStats{};
        self.budget_backoff.beginPass();
        self.coalescing.beginPass();
        for (namespaces) |namespace| {
            try cancellation.check();
            self.coalescing.touch(namespace.name);
            const now = std.Io.Timestamp.now(self.io, .awake).toNanoseconds();
            // Eligibility precedes all document loading and impact prediction.
            if (self.budget_backoff.blocked(namespace.name, now)) {
                self.coalescing.suppressWake(namespace.name);
                stats.budget_rejected_namespaces += 1;
                continue;
            }
            var status = self.catalog.buildStatusUntil(namespace.name, cancellation) catch |err| switch (err) {
                error.LakeSidecarBuildBudgetExceeded => {
                    self.coalescing.suppressWake(namespace.name);
                    try self.budget_backoff.reject(self.alloc, namespace.name, std.Io.Timestamp.now(self.io, .awake).toNanoseconds());
                    stats.budget_rejected_namespaces += 1;
                    std.log.warn("serverless prediction budget exceeded namespace={s}; retaining published head and backing off", .{namespace.name});
                    continue;
                },
                error.FileNotFound => {
                    self.coalescing.clear(namespace.name);
                    stats.idle_namespaces += 1;
                    continue;
                },
                else => return err,
            };
            defer status.deinit(self.alloc);
            const coalesced_tail_ready = if (!status.publish_recommended and status.pending_records > 0 and
                status.enrichment_active_stage != null and status.pending_records < status.enrichment_publish_min_pending_records)
                try self.coalescing.ready(self.alloc, namespace.name, status.published_wal_end_lsn, self.scheduling_clock.now(self.io), self.max_coalescing_delay_ms)
            else clear: {
                self.coalescing.clear(namespace.name);
                break :clear false;
            };
            if (!status.publish_recommended and !coalesced_tail_ready) {
                self.budget_backoff.clear(namespace.name);
                stats.idle_namespaces += 1;
                continue;
            }

            var held_lease: ?work_lease.HeldLease = null;
            var held_bootstrap_lease: ?work_lease.HeldBootstrapLease = null;
            // Bootstrap and established publications use the same durable
            // HEAD coordination record and monotonically increasing fence.
            if (self.lease_provider) |provider| {
                if (status.head_version == 0) {
                    held_bootstrap_lease = try work_lease.acquireBootstrapHeld(
                        provider,
                        self.io,
                        namespace.name,
                        self.lease_owner_id orelse return error.MissingLeaseOwner,
                        self.lease_ttl_ns,
                    );
                    if (held_bootstrap_lease == null) {
                        stats.lease_conflicts += 1;
                        continue;
                    }
                    if (held_bootstrap_lease.?.acquisition.took_over) stats.lease_takeovers += 1;
                } else {
                    held_lease = try work_lease.acquireHeld(
                        provider,
                        self.io,
                        namespace.name,
                        self.lease_owner_id orelse return error.MissingLeaseOwner,
                        self.lease_ttl_ns,
                    );
                    if (held_lease == null) {
                        stats.lease_conflicts += 1;
                        continue;
                    }
                    if (held_lease.?.acquisition.took_over) stats.lease_takeovers += 1;
                }
            }
            defer if (held_bootstrap_lease) |*lease| {
                _ = lease.release() catch {};
            };
            defer if (held_lease) |*lease| {
                _ = lease.release() catch {};
            };

            const publication_guard = if (held_lease) |*lease|
                lease.guard()
            else if (held_bootstrap_lease) |*lease|
                lease.guard()
            else
                null;
            const build_cancellation = if (held_lease) |*lease|
                lease.cancellation(cancellation)
            else if (held_bootstrap_lease) |*lease|
                lease.cancellation(cancellation)
            else
                cancellation;
            var result = self.catalog.buildNamespaceGuardedUntil(
                namespace.name,
                publication_guard,
                build_cancellation,
            ) catch |err| switch (err) {
                error.LakeSidecarBuildBudgetExceeded => {
                    // Deterministic per-namespace admission must not terminate
                    // the publisher or hot-loop an expensive failed build.
                    try self.budget_backoff.reject(self.alloc, namespace.name, std.Io.Timestamp.now(self.io, .awake).toNanoseconds());
                    stats.budget_rejected_namespaces += 1;
                    std.log.warn("serverless sidecar build budget exceeded namespace={s}; retaining published head and backing off", .{namespace.name});
                    continue;
                },
                error.HeadChanged => {
                    stats.head_conflicts += 1;
                    continue;
                },
                error.WorkLeaseLost => {
                    // Takeover after acquisition is expected contention. The
                    // stale worker is fenced at publication and the publisher
                    // must remain available for later namespaces and ticks.
                    stats.lease_conflicts += 1;
                    continue;
                },
                error.FileNotFound => {
                    stats.idle_namespaces += 1;
                    continue;
                },
                else => return err,
            };
            defer result.deinit(self.alloc);
            self.budget_backoff.clear(namespace.name);
            self.coalescing.clear(namespace.name);
            if (result.published) {
                stats.published_namespaces += 1;
            } else {
                // Another publisher may win between buildStatus() and buildNamespace().
                // Treat the resulting no-op like an idle namespace rather than losing it
                // from the run accounting entirely.
                stats.idle_namespaces += 1;
            }
        }

        self.budget_backoff.endPass();
        self.coalescing.endPass();
        return stats;
    }

    fn runLoop(self: *BackgroundPublisher) void {
        while (!self.stop_requested.load(.monotonic)) {
            _ = self.runOnceUntil(&self.stop_requested) catch |err| {
                if (err == error.Canceled and self.stop_requested.load(.acquire)) return;
                lockAtomic(&self.failure_mu);
                if (self.run_failure == null) self.run_failure = err;
                self.failure_mu.unlock();
                return;
            };
            self.idle_waiting.store(true, .release);
            defer self.idle_waiting.store(false, .release);
            // A long ordinary poll must not stretch a short coalescing bound.
            // Expired entries already attempted this pass use ordinary polling
            // on contention/admission failures, rather than spinning at zero.
            const wait_ms = self.nextWakeDelayMs(self.poll_interval_ms);
            self.stop_wake.waitTimeout(self.io, .{ .duration = .{
                .raw = .fromMilliseconds(@intCast(wait_ms)),
                .clock = .awake,
            } }) catch |err| switch (err) {
                error.Timeout => continue,
                error.Canceled => return,
            };
            return;
        }
    }
};

pub const SchedulingClock = struct {
    ptr: ?*anyopaque = null,
    now_fn: ?*const fn (*anyopaque) i96 = null,

    fn now(self: SchedulingClock, io: std.Io) i96 {
        if (self.ptr) |ptr| if (self.now_fn) |read| return read(ptr);
        return std.Io.Timestamp.now(io, .awake).toNanoseconds();
    }
};

/// One bounded deadline per live namespace with an under-target pending tail.
/// More arrivals and metadata-only HEAD changes never extend that deadline.
/// Resource admission is separate: expiry grants eligibility, not authority
/// to bypass budgets or publication leases.
const PublicationCoalescing = struct {
    const Entry = struct { published_lsn: u64, until_ns: i96, seen: bool = true, attempted: bool = false };
    entries: std.AutoHashMapUnmanaged([32]u8, Entry) = .empty,

    fn deinit(self: *@This(), alloc: Allocator) void {
        self.entries.deinit(alloc);
    }
    fn beginPass(self: *@This()) void {
        var values = self.entries.valueIterator();
        while (values.next()) |value| value.seen = false;
    }
    fn touch(self: *@This(), namespace: []const u8) void {
        if (self.entries.getPtr(BudgetBackoff.key(namespace))) |entry| entry.seen = true;
    }
    fn endPass(self: *@This()) void {
        var entries = self.entries.iterator();
        while (entries.next()) |entry| if (!entry.value_ptr.seen) {
            _ = self.entries.remove(entry.key_ptr.*);
        };
    }
    fn clear(self: *@This(), namespace: []const u8) void {
        _ = self.entries.remove(BudgetBackoff.key(namespace));
    }
    fn suppressWake(self: *@This(), namespace: []const u8) void {
        if (self.entries.getPtr(BudgetBackoff.key(namespace))) |entry| entry.attempted = true;
    }
    fn ready(self: *@This(), alloc: Allocator, namespace: []const u8, published_lsn: u64, now: i96, delay_ms: u64) !bool {
        const entry = try self.entries.getOrPut(alloc, BudgetBackoff.key(namespace));
        if (!entry.found_existing or entry.value_ptr.published_lsn != published_lsn)
            entry.value_ptr.* = .{ .published_lsn = published_lsn, .until_ns = now +| @as(i96, delay_ms) * std.time.ns_per_ms };
        entry.value_ptr.seen = true;
        const eligible = now >= entry.value_ptr.until_ns;
        if (eligible) entry.value_ptr.attempted = true;
        return eligible;
    }
    fn nextWaitMs(self: *@This(), now: i96, poll_ms: u64) u64 {
        var wait = poll_ms;
        var entries = self.entries.valueIterator();
        while (entries.next()) |entry| {
            if (entry.until_ns <= now) {
                // Expiry may occur while another namespace is still building.
                // Only contention/admission after an attempt may defer retry.
                if (!entry.attempted) return 1;
                continue;
            }
            const ns = entry.until_ns -| now;
            const ms = @divTrunc(ns +| (std.time.ns_per_ms - 1), std.time.ns_per_ms);
            wait = @min(wait, std.math.cast(u64, ms) orelse std.math.maxInt(u64));
        }
        return @max(wait, 1);
    }
};

/// Process-local retry scheduling, not durable publication authority. There is
/// one entry per rejected live namespace, never a FIFO that evicts an active
/// deadline. Successful catalog passes reclaim deleted namespaces. Memory is
/// O(catalog namespace count), independent of the number of failed attempts.
/// Explicit build requests bypass scheduling. A changed policy/source is
/// eligible for retry within a minute, even without a new WAL record or restart.
const BudgetBackoff = struct {
    const Entry = struct { until_ns: i96, attempts: u8, seen: bool = true };
    entries: std.AutoHashMapUnmanaged([32]u8, Entry) = .empty,

    fn deinit(self: *BudgetBackoff, alloc: Allocator) void {
        self.entries.deinit(alloc);
    }

    fn beginPass(self: *BudgetBackoff) void {
        var values = self.entries.valueIterator();
        while (values.next()) |entry| entry.seen = false;
    }

    fn endPass(self: *BudgetBackoff) void {
        var entries = self.entries.iterator();
        // remove does not relocate entries or invalidate the iterator.
        while (entries.next()) |entry| {
            if (!entry.value_ptr.seen) _ = self.entries.remove(entry.key_ptr.*);
        }
    }

    fn key(namespace: []const u8) [32]u8 {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(namespace, &digest, .{});
        return digest;
    }

    fn blocked(self: *BudgetBackoff, namespace: []const u8, now: i96) bool {
        const entry = self.entries.getPtr(key(namespace)) orelse return false;
        entry.seen = true;
        return now < entry.until_ns;
    }

    fn clear(self: *BudgetBackoff, namespace: []const u8) void {
        _ = self.entries.remove(key(namespace));
    }

    fn reject(self: *BudgetBackoff, alloc: Allocator, namespace: []const u8, now: i96) !void {
        const entry = try self.entries.getOrPut(alloc, key(namespace));
        const attempts = @min(@as(u8, if (entry.found_existing) entry.value_ptr.attempts else 0) + 1, 4);
        const delay_seconds: i96 = @min(@as(i96, 5) << @intCast(attempts), 60);
        entry.value_ptr.* = .{ .until_ns = now +| delay_seconds * std.time.ns_per_s, .attempts = attempts };
    }
};

test "serverless publication budget backoff isolates namespaces and retries within a minute" {
    var backoff = BudgetBackoff{};
    defer backoff.deinit(std.testing.allocator);
    try backoff.reject(std.testing.allocator, "large", 0);
    try std.testing.expect(backoff.blocked("large", 0));
    try std.testing.expect(!backoff.blocked("small", 0));
    try std.testing.expect(!backoff.blocked("large", 10 * std.time.ns_per_s));
    for (0..10) |_| try backoff.reject(std.testing.allocator, "large", 0);
    try std.testing.expect(backoff.blocked("large", 59 * std.time.ns_per_s));
    try std.testing.expect(!backoff.blocked("large", 60 * std.time.ns_per_s));
    backoff.clear("large");
    try std.testing.expect(!backoff.blocked("large", 0));
}

test "serverless publication coalescing keeps bounded deadlines and reclaims deleted namespaces" {
    const a = std.testing.allocator;
    var pending = PublicationCoalescing{};
    defer pending.deinit(a);
    try std.testing.expect(!try pending.ready(a, "docs", 5, 0, 1000));
    try std.testing.expectEqual(@as(u64, 1000), pending.nextWaitMs(0, 60_000));
    try std.testing.expect(!try pending.ready(a, "docs", 5, 999 * std.time.ns_per_ms, 1000));
    try std.testing.expectEqual(@as(u64, 1), pending.nextWaitMs(std.time.ns_per_s, 60_000));
    try std.testing.expect(try pending.ready(a, "docs", 5, std.time.ns_per_s, 1000));
    try std.testing.expectEqual(@as(u64, 60_000), pending.nextWaitMs(std.time.ns_per_s, 60_000));
    try std.testing.expect(!try pending.ready(a, "docs", 6, std.time.ns_per_s, 1000));
    try std.testing.expect(try pending.ready(a, "immediate", 0, 0, 0));
    pending.beginPass();
    pending.touch("docs");
    pending.endPass();
    try std.testing.expectEqual(@as(usize, 1), pending.entries.count());
    pending.clear("docs");
    try std.testing.expectEqual(@as(usize, 0), pending.entries.count());
}

test "serverless background publisher drains small WAL and enrichment batches after bounded coalescing" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/coalescing", .{tmp.sub_path});
    defer a.free(path);
    const artifact_path = try std.fs.path.join(a, &.{ path, "artifacts" });
    defer a.free(artifact_path);
    const manifest_path = try std.fs.path.join(a, &.{ path, "manifests" });
    defer a.free(manifest_path);
    const wal_path = try std.fs.path.join(a, &.{ path, "wal" });
    defer a.free(wal_path);
    const catalog_path = try std.fs.path.join(a, &.{ path, "catalog" });
    defer a.free(catalog_path);
    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(a, artifact_path);
    var artifacts = fs_artifacts.artifactStore();
    defer artifacts.deinit();
    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(a, manifest_path);
    var manifests = fs_manifests.manifestStore();
    defer manifests.deinit();
    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(a, manifest_path);
    var progress = fs_progress.progressStore();
    defer progress.deinit();
    var fs_wal = try @import("../wal/mod.zig").FsStore.init(a, wal_path);
    var wal = fs_wal.walStore();
    defer wal.deinit();
    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(a, catalog_path);
    var store = fs_catalog.catalogStore();
    defer store.deinit();
    var builder = @import("builder.zig").Builder.init(a, &artifacts, &manifests, &progress, &wal);
    builder.setIo(std.testing.io);
    var catalog = catalog_service.CatalogService.init(a, &artifacts, &manifests, &progress, &wal, &builder, &store);
    defer catalog.deinit();
    _ = try catalog.ensureNamespace("docs", 1);
    _ = try catalog.setPolicy("docs", .{ .enrichment_enabled = true, .enrichment_batch_size = 1, .enrichment_publish_min_pending_records = 64 });
    var api = @import("../api/service.zig").Service.init(a, &wal, &builder);
    var seed = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 1, .mutations = &.{.{ .kind = .upsert, .doc_id = "empty", .body = "{\"text\":\"\"}" }} });
    defer seed.deinit(a);
    var bootstrap = try catalog.buildNamespace("docs");
    defer bootstrap.deinit(a);
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 1, .mutations = &.{
        .{ .kind = .upsert, .doc_id = "a", .body = "{\"text\":\"alpha\"}" },
        .{ .kind = .upsert, .doc_id = "b", .body = "{\"text\":\"bravo\"}" },
    } });
    defer ingest.deinit(a);
    const Clock = struct {
        ns: i96 = 0,
        fn read(ptr: *anyopaque) i96 {
            return @as(*@This(), @ptrCast(@alignCast(ptr))).ns;
        }
    };
    var clock = Clock{};
    var publisher = BackgroundPublisher.init(a, std.testing.io, &catalog, 60_000);
    defer publisher.deinit();
    publisher.scheduling_clock = .{ .ptr = &clock, .now_fn = Clock.read };
    var before = try catalog.buildStatus("docs");
    defer before.deinit(a);
    try std.testing.expect(!before.publish_recommended);
    try std.testing.expectEqual(@as(usize, 0), (try publisher.runOnce()).published_namespaces);
    try std.testing.expectEqual(@as(u64, 1000), publisher.nextWakeDelayMs(60_000));
    clock.ns = 500 * std.time.ns_per_ms;
    var arrival = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 2, .mutations = &.{.{ .kind = .upsert, .doc_id = "c", .body = "{\"text\":\"charlie\"}" }} });
    defer arrival.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), (try publisher.runOnce()).published_namespaces);
    clock.ns = std.time.ns_per_s;
    try std.testing.expectEqual(@as(usize, 1), (try publisher.runOnce()).published_namespaces);
    var enricher = @import("../enrichment/worker.zig").SparseEnricher.init(a, &artifacts, &manifests, &progress, &wal);
    defer enricher.deinit();
    try std.testing.expectEqual(@as(usize, 1), (try enricher.runNamespaceWithConfig("docs", .{ .batch_size = 1 })).wal_appends);
    try std.testing.expectEqual(@as(usize, 1), (try enricher.runNamespaceWithConfig("docs", .{ .batch_size = 1 })).idle_namespaces);
    try std.testing.expectEqual(@as(usize, 0), (try publisher.runOnce()).published_namespaces);
    // Deadline expiry never overrides an independent admission rejection.
    try publisher.budget_backoff.reject(a, "docs", std.Io.Timestamp.now(std.testing.io, .awake).toNanoseconds());
    clock.ns = 2 * std.time.ns_per_s;
    try std.testing.expectEqual(@as(usize, 1), (try publisher.runOnce()).budget_rejected_namespaces);
    publisher.budget_backoff.clear("docs");
    try std.testing.expectEqual(@as(usize, 1), (try publisher.runOnce()).published_namespaces);
    try std.testing.expectEqual(@as(usize, 1), (try enricher.runNamespaceWithConfig("docs", .{ .batch_size = 1 })).wal_appends);
    publisher.max_coalescing_delay_ms = 0;
    try std.testing.expectEqual(@as(usize, 1), (try publisher.runOnce()).published_namespaces);
    try std.testing.expectEqual(@as(usize, 0), publisher.coalescing.entries.count());
}

test "serverless publication retry scheduling survives namespace cardinality and reclaims deletions" {
    var backoff = BudgetBackoff{};
    defer backoff.deinit(std.testing.allocator);
    var name_buf: [32]u8 = undefined;
    for ([_]usize{ 65, 128, 4096 }) |count| {
        backoff.beginPass();
        for (0..count) |i| {
            const name = try std.fmt.bufPrint(&name_buf, "namespace-{d}", .{i});
            try backoff.reject(std.testing.allocator, name, 0);
        }
        backoff.endPass();
        for (0..3) |_| {
            backoff.beginPass();
            for (0..count) |i| {
                const name = try std.fmt.bufPrint(&name_buf, "namespace-{d}", .{i});
                try std.testing.expect(backoff.blocked(name, std.time.ns_per_s));
            }
            backoff.endPass();
            try std.testing.expectEqual(count, backoff.entries.count());
        }
    }
    backoff.beginPass();
    try std.testing.expect(backoff.blocked("namespace-0", 0));
    backoff.endPass();
    try std.testing.expectEqual(@as(usize, 1), backoff.entries.count());
}

test "serverless background publisher publishes once and stop wakes a long idle wait" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-run-once");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-run-once");
    const wal_root = tmpPath(&wal_root_buf, "wal-run-once");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-run-once");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = @import("builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = catalog_service.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    _ = try catalog.ensureNamespace("docs", 100);

    const mutation = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    var ingest = try api.ingestBatch(.{
        .namespace = "docs",
        .timestamp_ns = 200,
        .mutations = &mutation,
    });
    defer ingest.deinit(alloc);

    var io_impl = threadedIo();
    defer io_impl.deinit();
    var publisher = BackgroundPublisher.initWithIo(
        alloc,
        io_impl.io(),
        &catalog,
        std.math.maxInt(u32),
    );
    defer publisher.deinit();
    const published = try publisher.runOnce();
    try std.testing.expectEqual(@as(usize, 1), published.published_namespaces);
    try std.testing.expectEqual(@as(usize, 0), published.head_conflicts);
    try std.testing.expectEqual(@as(u64, 1), try progress_store.getHead("docs"));

    _ = try catalog.ensureNamespace("healthy", 100);
    for ([_][]const u8{ "docs", "healthy" }) |namespace| {
        var pending = try api.ingestBatch(.{ .namespace = namespace, .timestamp_ns = 300, .mutations = &mutation });
        pending.deinit(alloc);
    }
    const Reject = struct {
        fn reach(_: *anyopaque, event: @import("builder.zig").PublicationLifecycleEvent) !void {
            if (std.mem.eql(u8, event.namespace, "docs")) return error.LakeSidecarBuildBudgetExceeded;
        }
    };
    var hook_context: u8 = 0;
    builder.setPublicationLifecycleHook(.{ .ptr = &hook_context, .reach_fn = Reject.reach });
    const rejected = try publisher.runOnce();
    try std.testing.expectEqual(@as(usize, 1), rejected.budget_rejected_namespaces);
    try std.testing.expectEqual(@as(usize, 1), rejected.published_namespaces);
    try std.testing.expectEqual(@as(u64, 1), try progress_store.getHead("docs"));
    try std.testing.expect(publisher.runtimeFailure() == null);
    builder.setPublicationLifecycleHook(null);
    const throttled = try publisher.runOnce();
    try std.testing.expectEqual(@as(usize, 1), throttled.budget_rejected_namespaces);
    try std.testing.expectEqual(@as(usize, 0), throttled.published_namespaces);
    publisher.budget_backoff.clear("docs");
    const retried = try publisher.runOnce();
    try std.testing.expectEqual(@as(usize, 1), retried.published_namespaces);

    var canceled: std.atomic.Value(bool) = .init(true);
    try std.testing.expectError(error.Canceled, publisher.runOnceUntil(&canceled));

    try publisher.start();
    var attempts: usize = 0;
    while (!publisher.idle_waiting.load(.acquire) and attempts < 50) : (attempts += 1) sleepMs(5);
    try std.testing.expect(publisher.idle_waiting.load(.acquire));
    publisher.stop();
    try std.testing.expect(publisher.future == null);
}

test "serverless background publisher loop publishes asynchronously and latest reads remain valid" {
    const alloc = std.heap.page_allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-loop");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-loop");
    const wal_root = tmpPath(&wal_root_buf, "wal-loop");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-loop");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = @import("builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = catalog_service.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    _ = try catalog.ensureNamespace("docs", 100);

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const initial = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var first_ingest = try api.ingestBatch(.{
        .namespace = "docs",
        .timestamp_ns = 200,
        .mutations = &initial,
    });
    defer first_ingest.deinit(alloc);
    var first_build = try builder.publishNamespace("docs");
    defer first_build.deinit(alloc);

    const next = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "beta" },
    };
    var next_ingest = try api.ingestBatch(.{
        .namespace = "docs",
        .timestamp_ns = 300,
        .mutations = &next,
    });
    defer next_ingest.deinit(alloc);

    var publisher = BackgroundPublisher.init(alloc, std.testing.io, &catalog, 1);
    defer publisher.deinit();
    {
        var unavailable = std.Io.Threaded.init(alloc, .{ .concurrent_limit = .nothing });
        defer unavailable.deinit();
        publisher.io = unavailable.io();
        defer {
            publisher.stop();
            publisher.io = std.testing.io;
        }
        try std.testing.expectError(error.ConcurrencyUnavailable, publisher.start());
        try std.testing.expect(publisher.future == null);
    }
    publisher.poll_interval_ms = 60_000;
    try publisher.start();
    try std.testing.expectError(error.AlreadyStarted, publisher.start());

    var query = @import("../query/mod.zig").QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var latest_seen_tail = false;
    var attempts: usize = 0;
    while (attempts < 50) : (attempts += 1) {
        const head = progress_store.getHead("docs") catch 0;
        var session = try query.openHeadSession("docs");
        defer session.deinit();
        const tail = try wal_store.readFromAlloc("docs", session.manifest.wal_end_lsn + 1);
        defer @import("../wal/mod.zig").freeRecords(alloc, tail);
        try std.testing.expect(session.manifest.wal_end_lsn <= try wal_store.latestLsn("docs"));
        if (tail.len > 0) latest_seen_tail = true;
        if (head >= 2) break;
        sleepMs(5);
    }

    try std.testing.expect(latest_seen_tail);
    try std.testing.expectEqual(@as(u64, 2), try progress_store.getHead("docs"));
    publisher.stop();
    publisher.poll_interval_ms = 60_000;
    try publisher.start();
    publisher.stop();
    try std.testing.expect(publisher.future == null);
}

test "serverless concurrent background publishers yield a single publish winner" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-race");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-race");
    const wal_root = tmpPath(&wal_root_buf, "wal-race");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-race");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder_a = @import("builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var builder_b = @import("builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog_a = catalog_service.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder_a, &catalog_store);
    defer catalog_a.deinit();
    var catalog_b = catalog_service.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder_b, &catalog_store);
    defer catalog_b.deinit();
    _ = try catalog_a.ensureNamespace("docs", 100);

    const mutation = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder_a);
    var ingest = try api.ingestBatch(.{
        .namespace = "docs",
        .timestamp_ns = 200,
        .mutations = &mutation,
    });
    defer ingest.deinit(alloc);

    const RaceState = struct {
        pub_a: BackgroundPublisher,
        pub_b: BackgroundPublisher,
        stats_a: PublishRunStats = .{},
        stats_b: PublishRunStats = .{},
        error_a: ?anyerror = null,
        error_b: ?anyerror = null,

        fn runA(self: *@This()) void {
            self.stats_a = self.pub_a.runOnce() catch |err| {
                self.error_a = err;
                return;
            };
        }

        fn runB(self: *@This()) void {
            self.stats_b = self.pub_b.runOnce() catch |err| {
                self.error_b = err;
                return;
            };
        }
    };

    var state = RaceState{
        .pub_a = BackgroundPublisher.init(alloc, std.testing.io, &catalog_a, 1),
        .pub_b = BackgroundPublisher.init(alloc, std.testing.io, &catalog_b, 1),
    };
    defer state.pub_a.deinit();
    defer state.pub_b.deinit();
    var thread_a = try std.testing.io.concurrent(RaceState.runA, .{&state});
    defer thread_a.await(std.testing.io);
    var thread_b = try std.testing.io.concurrent(RaceState.runB, .{&state});
    thread_a.await(std.testing.io);
    thread_b.await(std.testing.io);

    try std.testing.expectEqual(@as(?anyerror, null), state.error_a);
    try std.testing.expectEqual(@as(?anyerror, null), state.error_b);
    try std.testing.expectEqual(@as(usize, 1), state.stats_a.published_namespaces + state.stats_b.published_namespaces);
    try std.testing.expectEqual(@as(usize, 1), state.stats_a.head_conflicts + state.stats_b.head_conflicts + state.stats_a.idle_namespaces + state.stats_b.idle_namespaces + state.stats_a.lease_conflicts + state.stats_b.lease_conflicts);
    try std.testing.expectEqual(@as(u64, 1), try progress_store.getHead("docs"));
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-coordinator-{s}-{d}-{d}\x00", .{
        label,
        nowNs(),
        nonce,
    }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

// Native-thread regression tests below intentionally use a real sleep while
// exercising the Threaded differential backend. Production loops sleep on the
// borrowed `std.Io` stored by `BackgroundPublisher`.
fn sleepMs(ms: u64) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.sleep(
        io_impl.io(),
        .fromMilliseconds(@intCast(@max(ms, 1))),
        .awake,
    ) catch {};
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}
