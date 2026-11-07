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

//! One mutable audit snapshot per DB. Entries own stable budget allocators;
//! no request allocator, cancellation pointer or apply lock survives a turn.
//! Lease/refcount retirement matches immutable schema/index preparation, with
//! exclusive page execution because an audit cursor itself is mutable.
const std = @import("std");
const job = @import("relational_nullability_job.zig");
const debt = @import("../transaction_debt.zig");
const DirectoryAudit = debt.SnapshotAudit(*@FieldType(@import("relational_rows.zig").Reader, "read"));
const TransactionAudit = @import("../transactions.zig").TxnManager.TopologySnapshotAudit(*@FieldType(@import("relational_rows.zig").Reader, "read"));
const HistoryAudit = @import("relational_schema_apply_outcome.zig").SnapshotAudit(*@FieldType(@import("relational_rows.zig").Reader, "read"));
const resources = @import("../resource_manager.zig");
const Runtime = @import("../background_runtime.zig").BackendRuntime;
const Scheduler = @import("../../common/maintenance_scheduler.zig").Scheduler;
const Context = @import("../../api/operation.zig").RequestContext;
const A = std.mem.Allocator;
const Digest = [32]u8;
const Control = struct {
    request: Context,
    pub fn check(control: *const @This()) !void {
        try control.request.ensureActive();
    }
};
const idle_ms = 30_000;
pub const max_resident_snapshots = 16;
var residents: std.atomic.Value(usize) = .init(0);

pub fn residentSnapshotCount() usize {
    return residents.load(.acquire);
}
fn reserveSlot() bool {
    var count = residents.load(.acquire);
    while (count < max_resident_snapshots) {
        if (residents.cmpxchgWeak(count, count + 1, .acq_rel, .acquire)) |changed| count = changed else return true;
    }
    return false;
}
fn nowMs(io: std.Io) u64 {
    return @intCast(@max(0, @divTrunc(std.Io.Clock.awake.now(io).nanoseconds, std.time.ns_per_ms)));
}
fn checkpoint(raw: ?*anyopaque) !void {
    const context: *const Context = @ptrCast(@alignCast(raw.?));
    try context.ensureActive();
}

pub const Cache = struct {
    mutex: std.Io.Mutex = .init,
    changed: std.Io.Condition = .init,
    io: ?std.Io = null,
    runtime: ?*Runtime = null,
    manager: ?*resources.ResourceManager = null,
    reclaimer: u64 = 0,
    idle_handle: ?Scheduler.Handle = null,
    entry: ?*Entry = null,
    building: bool = false,
    epoch: u64 = 0,
    retiring: bool = false,
    leases: std.atomic.Value(usize) = .init(0),
    preparations: u64 = 0,
    hits: u64 = 0,

    const Entry = struct {
        backing: A,
        header: ?resources.Reservation,
        budget: ?resources.BudgetedAllocator,
        refs: std.atomic.Value(usize) = .init(1),
        retired: std.atomic.Value(bool) = .init(false),
        work: std.Io.Mutex = .init,
        last_used_ms: std.atomic.Value(u64),
        physical_root: u128,
        header_digest: Digest,
        audit: ?job.RowAudit = null,
        directory: ?DirectoryAudit = null,
        transactions: ?TransactionAudit = null,
        history: ?HistoryAudit = null,
        initialized: bool = false,

        fn create(backing: A, manager: ?*resources.ResourceManager, physical_root: u128, digest: Digest, io: std.Io) !*Entry {
            if (!reserveSlot()) return error.ResourceBudgetExceeded;
            errdefer _ = residents.fetchSub(1, .acq_rel);
            var header = if (manager) |value| try value.reserve(.relational_preparation_working_set, @sizeOf(Entry)) else null;
            errdefer if (header) |*value| value.release();
            const entry = try backing.create(Entry);
            entry.* = .{ .backing = backing, .header = header, .budget = if (manager) |value| resources.BudgetedAllocator.initReclaiming(value, .relational_preparation_working_set, backing, 1) else null, .last_used_ms = .init(nowMs(io)), .physical_root = physical_root, .header_digest = digest };
            return entry;
        }
        fn matches(self: *const Entry, physical_root: u128, digest: Digest) bool {
            return self.physical_root == physical_root and std.mem.eql(u8, &self.header_digest, &digest);
        }
        fn retain(self: *Entry) void {
            const previous = self.refs.fetchAdd(1, .monotonic);
            std.debug.assert(previous > 0 and previous < std.math.maxInt(usize));
        }
        fn release(self: *Entry) void {
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            if (self.history) |*history| history.deinit();
            if (self.transactions) |*transactions| transactions.deinit();
            if (self.directory) |*directory| directory.deinit();
            if (self.audit) |*audit| audit.deinit();
            if (self.budget) |*budget| budget.deinit();
            var header = self.header;
            const backing = self.backing;
            backing.destroy(self);
            if (header) |*value| value.release();
            _ = residents.fetchSub(1, .acq_rel);
        }
        fn bytes(self: *const Entry) u64 {
            return (if (self.header) |value| value.bytes else @as(u64, 0)) +
                (if (self.budget) |value| value.reservation.bytes else @as(u64, 0));
        }
    };

    pub const Lease = struct {
        cache: *Cache,
        entry: *Entry,
        io: std.Io,
        pub fn audit(self: *const @This()) *job.RowAudit {
            return &self.entry.audit.?;
        }
        pub fn denied(self: *const @This()) bool {
            return if (self.entry.budget) |value| value.denied() else false;
        }
        pub fn invalid(self: *const @This()) bool {
            return self.audit().invalid or (if (self.entry.directory) |*value| value.invalid else false) or
                (if (self.entry.transactions) |*value| value.invalid else false) or
                (if (self.entry.history) |*value| value.invalid else false);
        }
        /// Structural directory coverage shares the row scanner's immutable
        /// image. This does not establish coordinator decisions or cohort ACKs.
        pub fn qualifyDirectory(self: *const @This(), a: A, budget: @import("relational_rows.zig").Budget, context: Context) !?debt.SnapshotStats {
            try self.requireCurrent();
            var points = try self.audit().reader.read.openReadScope(a);
            defer points.close();
            const control: Control = .{ .request = context };
            if (self.entry.directory == null) {
                const header = self.audit().header;
                const scope = if (header.admitted != null) try debt.Scope.fromBinding(header.binding) else null;
                self.entry.directory = try DirectoryAudit.init(&self.audit().reader.read, &points, scope, &control);
            }
            const directory = &self.entry.directory.?;
            if (!try directory.step(&points, .{ .records = @min(budget.records, debt.max_page_units), .time_ns = budget.time_ns, .io = self.io }, &control)) return null;
            return try directory.result();
        }
        pub fn qualifyTransactions(self: *const @This(), budget: @import("relational_rows.zig").Budget, scratch_dir: []const u8, context: Context) !?@import("../transaction_snapshot.zig").Stats {
            try self.requireCurrent();
            const control: Control = .{ .request = context };
            if (self.entry.transactions == null) {
                const a = if (self.entry.budget) |*value| value.allocator() else self.entry.backing;
                self.entry.transactions = try TransactionAudit.init(a, &self.audit().reader.read, .{ .io = self.io, .directory = scratch_dir, .resource_manager = self.cache.manager }, &control);
            }
            const transactions = &self.entry.transactions.?;
            if (!try transactions.step(.{ .units = @min(budget.records, debt.max_page_units), .time_ns = budget.time_ns, .io = self.io }, &control)) return null;
            return try transactions.result();
        }
        /// Exact schema-command history is qualified on the same retained
        /// image as rows and cohorts, independently of the row checkpoint.
        pub fn qualifyHistory(self: *const @This(), a: A, physical_root_dir: []const u8, budget: @import("relational_rows.zig").Budget, context: Context) !?@import("relational_schema_apply_outcome.zig").SnapshotStats {
            try self.requireCurrent();
            const control: Control = .{ .request = context };
            if (self.entry.history == null) {
                const ns = self.audit().header.binding.fence.namespace;
                const installed = try @import("native_backup_origin.zig").loadInstallation(a, self.io, physical_root_dir, self.audit().header.binding.fence.owner_group_id, @import("online_source_contract.zig").namespaceBytes(ns), self.entry.physical_root, context);
                const cut: ?@import("apply_receipts.zig").OrderedApplyReceipt = switch (self.audit().snapshot_cut) {
                    .raft => |value| .{ .term = value.term, .index = value.index },
                    .native => null,
                };
                const anchor = if (installed) |value| try value.historyAnchor(cut) else null;
                self.entry.history = try HistoryAudit.initAnchored(&self.audit().reader.read, ns, anchor, &control);
            }
            const history = &self.entry.history.?;
            if (!try history.step(.{ .records = @min(budget.records, 64), .time_ns = budget.time_ns, .io = self.io }, &control)) return null;
            return try history.result();
        }
        pub fn requireCurrent(self: *const @This()) !void {
            if (self.entry.retired.load(.acquire)) return error.SchemaValidationJobChanged;
        }
        pub fn seal(self: *const @This(), guard: *const @import("relational_schema_audit_checkpoint.zig").Guard, physical_root_dir: []const u8) !@import("relational_schema_root_evidence.zig").Evidence {
            try self.requireCurrent();
            if (self.entry.directory == null or self.entry.transactions == null or self.entry.history == null) return error.SchemaValidationNotReady;
            return try @import("relational_schema_root_evidence.zig").seal(guard, physical_root_dir, self.audit(), &self.entry.directory.?, &self.entry.transactions.?, &self.entry.history.?);
        }
        /// A failed older lease cannot evict its replacement. The retained
        /// reference makes this pointer identity immune to allocator reuse.
        pub fn discard(self: *const @This()) void {
            self.cache.mutex.lockUncancelable(self.io);
            self.entry.retired.store(true, .release);
            const cached = self.cache.entry == self.entry;
            if (cached) {
                self.cache.entry = null;
                self.cache.epoch +%= 1;
            }
            self.cache.mutex.unlock(self.io);
            if (cached) self.entry.release();
        }
        pub fn restoreCheckpoint(self: *const @This(), guard: *@import("relational_schema_audit_checkpoint.zig").Guard, context: Context) !void {
            var loaded = try guard.load();
            defer if (loaded) |*value| value.deinit();
            if (!self.entry.initialized) {
                var request_context = context;
                if (loaded) |*progress| try self.audit().resumeAuthenticatedCheckpoint(progress, .{ .context = &request_context, .checkpoint = checkpoint });
                self.entry.initialized = true;
            }
        }
        pub fn deinit(self: *@This()) void {
            self.entry.last_used_ms.store(nowMs(self.io), .release);
            self.entry.work.unlock(self.io);
            self.entry.release();
            _ = self.cache.leases.fetchSub(1, .acq_rel);
            self.* = undefined;
        }
    };

    fn lease(self: *Cache, entry: *Entry, io: std.Io, context: Context) !Lease {
        _ = self.leases.fetchAdd(1, .acq_rel);
        errdefer {
            entry.release();
            _ = self.leases.fetchSub(1, .acq_rel);
        }
        while (!entry.work.tryLock()) {
            try context.ensureActive();
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        errdefer entry.work.unlock(io);
        try context.ensureActive();
        if (entry.retired.load(.acquire)) return error.SchemaValidationJobChanged;
        return .{ .cache = self, .entry = entry, .io = io };
    }

    /// Header/physical-root identity is the cache key. Data writes after
    /// release must not replace the retained snapshot on every audit turn.
    pub fn acquire(self: *Cache, io: std.Io, backing: A, manager: ?*resources.ResourceManager, runtime: *Runtime, stable_address: bool, core: anytype, physical_root: u128, expected: job.Header, inputs: []const u8, context: Context) !Lease {
        try context.ensureActive();
        if (physical_root == 0) return error.SchemaValidationRuntimeRecoveryRequired;
        const digest = try expected.digest();
        const epoch = while (true) {
            try self.mutex.lock(io);
            var locked = true;
            errdefer if (locked) self.mutex.unlock(io);
            try context.ensureActive();
            if (self.retiring or (self.io != null and (self.manager != manager or self.runtime != runtime))) return error.SchemaValidationRuntimeRecoveryRequired;
            self.io = io;
            self.runtime = runtime;
            self.manager = manager;
            // Registrations are owned by the DB, not a transient request or
            // job attempt. Stack-opened owners use lazy expiry and close().
            if (stable_address) {
                if (self.idle_handle == null) self.idle_handle = try (try runtime.maintenanceScheduler()).register(self, idleStep);
                if (manager) |value| {
                    if (self.reclaimer == 0) self.reclaimer = try value.registerReclaimer(.relational_preparation_working_set, self, reclaim);
                }
            }
            if (self.entry) |entry| if (entry.matches(physical_root, digest) and nowMs(io) -| entry.last_used_ms.load(.acquire) < idle_ms) {
                entry.retain();
                self.hits +|= 1;
                self.mutex.unlock(io);
                locked = false;
                return self.lease(entry, io, context);
            };
            if (self.building) {
                self.changed.waitTimeout(io, &self.mutex, .{ .duration = .{ .raw = .fromMilliseconds(5), .clock = .awake } }) catch |err| switch (err) {
                    error.Timeout => {},
                    else => return err,
                };
                self.mutex.unlock(io);
                locked = false;
                continue;
            }
            if (inputs.len == 0) return error.SchemaValidationInputsRequired;
            self.building = true;
            const generation = self.epoch;
            const previous = self.entry;
            self.entry = null;
            self.mutex.unlock(io);
            if (previous) |entry| {
                entry.retired.store(true, .release);
                entry.release();
            }
            break generation;
        };
        var owns_build = true;
        errdefer if (owns_build) {
            self.mutex.lockUncancelable(io);
            self.building = false;
            self.changed.broadcast(io);
            self.mutex.unlock(io);
        };
        const entry = try Entry.create(backing, manager, physical_root, digest, io);
        var owned = true;
        errdefer if (owned) entry.release();
        var request_context = context;
        const a = if (entry.budget) |*value| value.allocator() else backing;
        entry.audit = job.RowAudit.init(a, core, expected.binding, inputs, .{ .context = &request_context, .checkpoint = checkpoint }) catch |err| {
            if (err == error.OutOfMemory) if (entry.budget) |value| if (value.denied()) return error.ResourceBudgetExceeded;
            return err;
        };
        if (!std.meta.eql(entry.audit.?.header, expected)) return error.SchemaValidationJobChanged;
        try context.ensureActive();
        if (entry.budget) |*value| _ = value.releaseUnusedCredit();
        self.mutex.lockUncancelable(io);
        if (self.epoch != epoch) {
            self.mutex.unlock(io);
            return error.SchemaValidationJobChanged;
        }
        entry.retain();
        self.entry = entry;
        self.preparations +|= 1;
        self.building = false;
        // A later lease cancellation must not clear another request's builder.
        // Publication relinquishes this request's preparation ownership.
        owns_build = false;
        self.changed.broadcast(io);
        self.mutex.unlock(io);
        runtime.wakeMaintenance(self);
        owned = false; // lease() consumes this reference on success AND error.
        return self.lease(entry, io, context);
    }

    pub fn evict(self: *Cache, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        const entry = self.entry;
        if (entry) |value| value.retired.store(true, .release);
        self.entry = null;
        self.epoch +%= 1;
        self.mutex.unlock(io);
        if (entry) |value| value.release();
    }

    fn reclaim(raw: *anyopaque, _: u64) u64 {
        const self: *Cache = @ptrCast(@alignCast(raw));
        if (!self.mutex.tryLock()) return 0;
        const entry = self.entry orelse {
            self.mutex.unlock(self.io.?);
            return 0;
        };
        if (entry.refs.load(.acquire) != 1) {
            self.mutex.unlock(self.io.?);
            return 0;
        }
        const bytes = entry.bytes();
        self.entry = null;
        self.mutex.unlock(self.io.?);
        entry.release();
        return bytes;
    }
    fn idleStep(self: *Cache) ?u64 {
        const io = self.io orelse return null;
        if (!self.mutex.tryLock()) return 100;
        const entry = self.entry orelse {
            self.mutex.unlock(io);
            return null;
        };
        const elapsed = nowMs(io) -| entry.last_used_ms.load(.acquire);
        if (elapsed < idle_ms or entry.refs.load(.acquire) != 1) {
            self.mutex.unlock(io);
            return if (elapsed < idle_ms) idle_ms - elapsed else 100;
        }
        self.entry = null;
        self.mutex.unlock(io);
        entry.release();
        return null;
    }
    pub fn deinit(self: *Cache, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        std.debug.assert(!self.building and self.leases.load(.acquire) == 0);
        self.retiring = true;
        const entry = self.entry;
        if (entry) |value| value.retired.store(true, .release);
        self.entry = null;
        var handle = self.idle_handle;
        self.idle_handle = null;
        const manager = self.manager;
        const reclaimer = self.reclaimer;
        self.reclaimer = 0;
        self.mutex.unlock(io);
        if (entry) |value| value.release();
        // Join callbacks without holding the mutex they inspect.
        if (handle) |*value| value.cancel(io);
        if (manager) |value| if (reclaimer != 0) value.unregisterReclaimer(reclaimer);
        self.io = null;
        self.runtime = null;
        self.manager = null;
    }
};

test "schema row-audit cache eviction retires leases without destroying their snapshots" {
    const io = std.testing.io;
    const baseline = residentSnapshotCount();
    var cache: Cache = .{ .io = io };
    defer cache.deinit(io);
    const old = try Cache.Entry.create(std.testing.allocator, null, 1, @splat(1), io);
    cache.entry = old;
    old.retain();
    var lease = try cache.lease(old, io, .{});
    defer lease.deinit();
    cache.evict(io);
    try std.testing.expectError(error.SchemaValidationJobChanged, lease.requireCurrent());
    try std.testing.expectEqual(baseline + 1, residentSnapshotCount());
    const replacement = try Cache.Entry.create(std.testing.allocator, null, 2, @splat(2), io);
    cache.entry = replacement;
    const epoch = cache.epoch;
    lease.discard();
    try std.testing.expect(cache.entry == replacement);
    try std.testing.expectEqual(epoch, cache.epoch);
    try std.testing.expect(!replacement.retired.load(.acquire));
    try std.testing.expectEqual(baseline + 2, residentSnapshotCount());
}

test "schema row-audit cache cancelled waiter releases only its reference and never the work lock" {
    const io = std.testing.io;
    var cache: Cache = .{ .io = io };
    defer cache.deinit(io);
    const entry = try Cache.Entry.create(std.testing.allocator, null, 1, @splat(1), io);
    cache.entry = entry;
    try entry.work.lock(io);
    defer entry.work.unlock(io);
    entry.retain();
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, cache.lease(entry, io, .{ .cancellation = @import("../../api/operation.zig").CancellationToken.fromAtomic(&canceled) }));
    try std.testing.expectEqual(@as(usize, 1), entry.refs.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), cache.leases.load(.acquire));
    try std.testing.expect(!entry.work.tryLock());
}

test "schema row-audit cache reclamation preserves active leases and returns admitted memory" {
    const io = std.testing.io;
    var manager = resources.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var cache: Cache = .{ .io = io, .manager = &manager };
    defer cache.deinit(io);
    const entry = try Cache.Entry.create(std.testing.allocator, &manager, 1, @splat(1), io);
    cache.entry = entry;
    entry.retain();
    var lease = try cache.lease(entry, io, .{});
    try std.testing.expectEqual(@as(u64, 0), Cache.reclaim(&cache, std.math.maxInt(u64)));
    try lease.requireCurrent();
    lease.deinit();
    const admitted = manager.sliceStats(.relational_preparation_working_set).used_bytes;
    try std.testing.expect(admitted >= @sizeOf(Cache.Entry));
    try std.testing.expectEqual(admitted, Cache.reclaim(&cache, std.math.maxInt(u64)));
    try std.testing.expect(cache.entry == null);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.relational_preparation_working_set).used_bytes);
}

test "schema row-audit cache idle retirement keeps live work and frees inactive snapshots" {
    const io = std.testing.io;
    var cache: Cache = .{ .io = io };
    defer cache.deinit(io);
    const entry = try Cache.Entry.create(std.testing.allocator, null, 1, @splat(1), io);
    cache.entry = entry;
    try std.testing.expect(Cache.idleStep(&cache).? <= idle_ms);
    entry.retain();
    var lease = try cache.lease(entry, io, .{});
    entry.last_used_ms.store(nowMs(io) -| idle_ms, .release);
    try std.testing.expect(Cache.idleStep(&cache) != null);
    try std.testing.expect(cache.entry == entry);
    lease.deinit();
    entry.last_used_ms.store(nowMs(io) -| idle_ms, .release);
    try std.testing.expect(Cache.idleStep(&cache) == null);
    try std.testing.expect(cache.entry == null);
}

test "schema row-audit cache allocation and admission failures return global snapshot slots" {
    const io = std.testing.io;
    const baseline = residentSnapshotCount();
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Cache.Entry.create(failing.allocator(), null, 1, @splat(1), io));
    try std.testing.expectEqual(baseline, residentSnapshotCount());
    var budgets = resources.Options.defaultBudgets();
    budgets[@backingInt(resources.Slice.relational_preparation_working_set)] = .{ .hard_limit_bytes = @sizeOf(Cache.Entry) - 1 };
    var manager = resources.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(std.testing.allocator);
    try std.testing.expectError(error.ResourceBudgetExceeded, Cache.Entry.create(std.testing.allocator, &manager, 1, @splat(1), io));
    try std.testing.expectEqual(baseline, residentSnapshotCount());
    var entries: [max_resident_snapshots]*Cache.Entry = undefined;
    var count: usize = 0;
    defer for (entries[0..count]) |entry| entry.release();
    while (residentSnapshotCount() < max_resident_snapshots) : (count += 1) {
        entries[count] = try Cache.Entry.create(std.testing.allocator, null, 1, @splat(1), io);
    }
    try std.testing.expectError(error.ResourceBudgetExceeded, Cache.Entry.create(std.testing.allocator, null, 1, @splat(1), io));
    for (entries[0..count]) |entry| entry.release();
    count = 0;
    try std.testing.expectEqual(baseline, residentSnapshotCount());
}
