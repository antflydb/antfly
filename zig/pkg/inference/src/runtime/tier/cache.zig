// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const platform = @import("antfly_platform");
const planner = @import("planner.zig");
const memory = @import("memory.zig");

pub const ResidencyTier = planner.ResidencyTier;

pub const Budget = struct {
    host_limit_bytes: usize = 0,
    backend_limit_bytes: usize = 0,
};

pub const Denial = struct {
    tier: ResidencyTier,
    current_bytes: usize,
    request_bytes: usize,
    requested_total_bytes: usize,
    limit_bytes: usize,
};

const AdmissionCredit = struct {
    lease: memory.AdmissionLease,
    tier: ResidencyTier,
};

pub const ReserveError = error{
    MemoryBudgetExceeded,
    ResourceTemporarilyUnavailable,
};

/// Exact process-wide ownership for physical cache growth beyond bytes already
/// covered by the model's resident lease. The binding is heap-stable because a
/// backend session may move its SharedCache value while it is being built.
const AdmissionBinding = struct {
    allocator: std.mem.Allocator,
    controller: *memory.AdmissionController,
    backend_class: memory.BackendClass,
    limits: memory.Limits,
    baseline: Budget,
    mutex: std.atomic.Mutex = .unlocked,
    host_bytes: usize = 0,
    backend_bytes: usize = 0,
    host_credited_bytes: usize = 0,
    backend_credited_bytes: usize = 0,
    credits: std.ArrayListUnmanaged(AdmissionCredit) = .empty,

    fn lock(self: *@This()) void {
        platform.sync.lockYielding(&self.mutex);
    }

    fn actualPtr(self: *@This(), tier: ResidencyTier) ?*usize {
        return switch (tier) {
            .disk => null,
            .host => &self.host_bytes,
            .backend => &self.backend_bytes,
        };
    }

    fn creditedPtr(self: *@This(), tier: ResidencyTier) ?*usize {
        return switch (tier) {
            .disk => null,
            .host => &self.host_credited_bytes,
            .backend => &self.backend_credited_bytes,
        };
    }

    fn baselineBytes(self: *const @This(), tier: ResidencyTier) usize {
        return switch (tier) {
            .disk => 0,
            .host => self.baseline.host_limit_bytes,
            .backend => self.baseline.backend_limit_bytes,
        };
    }

    fn amountsFor(tier: ResidencyTier, bytes: usize) memory.AdmissionAmounts {
        return switch (tier) {
            .disk => .{},
            .host => .{ .host_weight_bytes = bytes },
            .backend => .{ .backend_weight_bytes = bytes },
        };
    }

    fn acquireCredit(self: *@This(), tier: ResidencyTier, bytes: usize) ReserveError!void {
        if (bytes == 0) return;
        self.credits.ensureUnusedCapacity(self.allocator, 1) catch
            return error.ResourceTemporarilyUnavailable;
        const amounts = amountsFor(tier, bytes);
        var pressure: ?memory.AdmissionPressure = null;
        var lease = self.controller.tryAcquireWithPressure(
            self.backend_class,
            self.limits,
            amounts,
            true,
            &pressure,
        ) catch |err| return switch (err) {
            error.ResourceTemporarilyUnavailable => error.ResourceTemporarilyUnavailable,
            else => error.MemoryBudgetExceeded,
        };
        // Cache bytes remain resident after this growth operation. Settle the
        // transient live-pressure epoch while keeping exact policy ownership.
        lease.retain(amounts) catch {
            lease.release();
            return error.MemoryBudgetExceeded;
        };
        self.credits.appendAssumeCapacity(.{ .lease = lease, .tier = tier });
    }

    fn releaseCredit(self: *@This(), tier: ResidencyTier, bytes: usize) void {
        var remaining = bytes;
        var index = self.credits.items.len;
        while (remaining > 0 and index > 0) {
            index -= 1;
            const credit = &self.credits.items[index];
            if (credit.tier != tier) continue;
            const owned = switch (tier) {
                .disk => 0,
                .host => credit.lease.amounts.host_weight_bytes,
                .backend => credit.lease.amounts.backend_weight_bytes,
            };
            std.debug.assert(owned > 0);
            if (owned <= remaining) {
                remaining -= owned;
                credit.lease.release();
                _ = self.credits.orderedRemove(index);
            } else {
                credit.lease.retain(amountsFor(tier, owned - remaining)) catch
                    @panic("cache admission lease rejected a valid reduction");
                remaining = 0;
            }
        }
        std.debug.assert(remaining == 0);
    }

    fn grow(self: *@This(), tier: ResidencyTier, bytes: usize) ReserveError!void {
        if (tier == .disk or bytes == 0) return;
        self.lock();
        defer self.mutex.unlock();

        const actual = self.actualPtr(tier).?;
        const credited = self.creditedPtr(tier).?;
        const next = std.math.add(usize, actual.*, bytes) catch
            return error.MemoryBudgetExceeded;
        const desired_credit = next -| self.baselineBytes(tier);
        if (desired_credit > credited.*) {
            try self.acquireCredit(tier, desired_credit - credited.*);
            credited.* = desired_credit;
        }
        actual.* = next;
    }

    fn release(self: *@This(), tier: ResidencyTier, bytes: usize) void {
        if (tier == .disk or bytes == 0) return;
        self.lock();
        defer self.mutex.unlock();

        const actual = self.actualPtr(tier).?;
        const credited = self.creditedPtr(tier).?;
        std.debug.assert(bytes <= actual.*);
        const next = actual.* - bytes;
        const desired_credit = next -| self.baselineBytes(tier);
        if (desired_credit < credited.*) {
            self.releaseCredit(tier, credited.* - desired_credit);
            credited.* = desired_credit;
        }
        actual.* = next;
    }

    fn deinit(self: *@This()) void {
        self.lock();
        std.debug.assert(self.host_bytes == 0);
        std.debug.assert(self.backend_bytes == 0);
        std.debug.assert(self.host_credited_bytes == 0);
        std.debug.assert(self.backend_credited_bytes == 0);
        std.debug.assert(self.credits.items.len == 0);
        self.credits.deinit(self.allocator);
        const allocator = self.allocator;
        self.mutex.unlock();
        allocator.destroy(self);
    }
};

pub const SharedCache = struct {
    budget: Budget,
    /// Serving policy installs non-zero ceilings before the session is
    /// published. Architecture-specific floors may widen auto-derived cache
    /// defaults, but must never widen past an explicit hard limit.
    hard_budget: ?Budget = null,
    host_bytes: usize = 0,
    backend_bytes: usize = 0,
    host_loads: u64 = 0,
    backend_loads: u64 = 0,
    host_evictions: u64 = 0,
    backend_evictions: u64 = 0,
    denials: u64 = 0,
    last_denial: ?Denial = null,
    admission: ?*AdmissionBinding = null,

    pub fn init(budget: Budget) SharedCache {
        return .{ .budget = budget };
    }

    /// Install process-owner limits before the cache becomes observable by a
    /// request. Zero retains the auto-derived limit for that tier.
    pub fn configureHardBudget(self: *SharedCache, hard_budget: Budget) void {
        std.debug.assert(self.host_bytes == 0);
        std.debug.assert(self.backend_bytes == 0);
        self.hard_budget = hard_budget;
        if (hard_budget.host_limit_bytes != 0)
            self.budget.host_limit_bytes = hard_budget.host_limit_bytes;
        if (hard_budget.backend_limit_bytes != 0)
            self.budget.backend_limit_bytes = hard_budget.backend_limit_bytes;
    }

    /// Bind post-load cache growth to the same aggregate controller used by
    /// model and request leases. `baseline` is the portion already owned by the
    /// model's resident lease, so mapped source weights are never double-counted
    /// while prepared layouts and promoted weights remain exact.
    pub fn configureAdmission(
        self: *SharedCache,
        allocator: std.mem.Allocator,
        controller: *memory.AdmissionController,
        backend_class: memory.BackendClass,
        limits: memory.Limits,
        baseline: Budget,
    ) !void {
        std.debug.assert(self.host_bytes == 0);
        std.debug.assert(self.backend_bytes == 0);
        std.debug.assert(self.admission == null);
        const binding = try allocator.create(AdmissionBinding);
        binding.* = .{
            .allocator = allocator,
            .controller = controller,
            .backend_class = backend_class,
            .limits = limits,
            .baseline = baseline,
        };
        self.admission = binding;
    }

    /// Reserve aggregate and live capacity before allocating cache memory.
    /// Callers must release the reservation if the subsequent allocation fails.
    pub fn reserve(self: *SharedCache, tier: ResidencyTier, bytes: usize) ReserveError!void {
        if (tier == .disk or bytes == 0) return;
        if (!self.canFitAdditional(tier, bytes)) return error.MemoryBudgetExceeded;
        if (self.admission) |binding| {
            try binding.grow(tier, bytes);
        }
        self.noteResidentUnchecked(tier, bytes);
    }

    /// Query the serving owner's current physical host-pressure signal. This
    /// is deliberately independent of cache geometry: mmap page faults can
    /// increase cgroup working set without changing `host_bytes`.
    pub fn isLiveHostUnderPressure(self: *const SharedCache) bool {
        const binding = self.admission orelse return false;
        return binding.controller.isLiveHostUnderPressure();
    }

    pub fn noteResident(self: *SharedCache, tier: ResidencyTier, bytes: usize) void {
        // Serving sessions use tryReserve so process ownership is established
        // before memory grows. Direct/offline runtimes intentionally retain the
        // lightweight counter-only API.
        std.debug.assert(self.admission == null);
        self.noteResidentUnchecked(tier, bytes);
    }

    fn noteResidentUnchecked(self: *SharedCache, tier: ResidencyTier, bytes: usize) void {
        switch (tier) {
            .disk => {},
            .host => {
                self.host_bytes += bytes;
                self.host_loads += 1;
            },
            .backend => {
                self.backend_bytes += bytes;
                self.backend_loads += 1;
            },
        }
    }

    pub fn noteRelease(self: *SharedCache, tier: ResidencyTier, bytes: usize) void {
        if (self.admission) |binding| binding.release(tier, bytes);
        switch (tier) {
            .disk => {},
            .host => {
                self.host_bytes -|= bytes;
                self.host_evictions += 1;
            },
            .backend => {
                self.backend_bytes -|= bytes;
                self.backend_evictions += 1;
            },
        }
    }

    /// Release the binding after every physical cache allocation has been
    /// destroyed. Ordinary eviction reports exact releases incrementally;
    /// session teardown drains any entries that remained resident at close.
    pub fn deinitAdmission(self: *SharedCache) void {
        const binding = self.admission orelse return;
        if (self.host_bytes != 0) binding.release(.host, self.host_bytes);
        if (self.backend_bytes != 0) binding.release(.backend, self.backend_bytes);
        self.host_bytes = 0;
        self.backend_bytes = 0;
        self.admission = null;
        binding.deinit();
    }

    pub fn isOverBudget(self: *const SharedCache, tier: ResidencyTier) bool {
        return switch (tier) {
            .disk => false,
            .host => self.budget.host_limit_bytes != 0 and self.host_bytes > self.budget.host_limit_bytes,
            .backend => self.budget.backend_limit_bytes != 0 and self.backend_bytes > self.budget.backend_limit_bytes,
        };
    }

    pub fn canFitAdditional(self: *const SharedCache, tier: ResidencyTier, bytes: usize) bool {
        return switch (tier) {
            .disk => true,
            .host => fitsAdditional(self.host_bytes, bytes, self.budget.host_limit_bytes),
            .backend => fitsAdditional(self.backend_bytes, bytes, self.budget.backend_limit_bytes),
        };
    }

    pub fn noteDenied(self: *SharedCache, tier: ResidencyTier, bytes: usize) void {
        if (tier == .disk) return;
        const current_bytes, const limit_bytes = switch (tier) {
            .disk => unreachable,
            .host => .{ self.host_bytes, self.budget.host_limit_bytes },
            .backend => .{ self.backend_bytes, self.budget.backend_limit_bytes },
        };
        self.denials += 1;
        self.last_denial = .{
            .tier = tier,
            .current_bytes = current_bytes,
            .request_bytes = bytes,
            .requested_total_bytes = std.math.add(usize, current_bytes, bytes) catch
                std.math.maxInt(usize),
            .limit_bytes = limit_bytes,
        };
    }

    pub fn lastDenialString(self: *const SharedCache, buf: []u8) ![]const u8 {
        const denial = self.last_denial orelse {
            return std.fmt.bufPrint(buf, "shared tier cache memory budget exceeded", .{});
        };
        return std.fmt.bufPrint(
            buf,
            "shared tier cache budget exceeded: tier={s} current={d} request={d} next={d} limit={d}",
            .{
                @tagName(denial.tier),
                denial.current_bytes,
                denial.request_bytes,
                denial.requested_total_bytes,
                denial.limit_bytes,
            },
        );
    }

    pub fn widenToAtLeast(self: *SharedCache, floor: Budget) void {
        self.budget.host_limit_bytes = boundedFloor(
            self.budget.host_limit_bytes,
            floor.host_limit_bytes,
            if (self.hard_budget) |hard| hard.host_limit_bytes else 0,
        );
        self.budget.backend_limit_bytes = boundedFloor(
            self.budget.backend_limit_bytes,
            floor.backend_limit_bytes,
            if (self.hard_budget) |hard| hard.backend_limit_bytes else 0,
        );
    }
};

fn fitsAdditional(current: usize, additional: usize, limit: usize) bool {
    const next = std.math.add(usize, current, additional) catch return false;
    return limit == 0 or next <= limit;
}

fn boundedFloor(current: usize, floor: usize, hard_limit: usize) usize {
    const widened = @max(current, floor);
    return if (hard_limit == 0) widened else @min(widened, hard_limit);
}

pub fn defaultBudgetForBackend(backend: planner.BackendClass) Budget {
    const limits = memory.defaultLimitsForBackend(backend);
    return .{
        .host_limit_bytes = limits.host_limit_bytes,
        .backend_limit_bytes = limits.backend_limit_bytes,
    };
}

test "shared cache tracks host and backend budgets" {
    var cache = SharedCache.init(.{
        .host_limit_bytes = 100,
        .backend_limit_bytes = 50,
    });

    cache.noteResident(.host, 70);
    cache.noteResident(.backend, 30);
    try std.testing.expect(!cache.isOverBudget(.host));
    try std.testing.expect(!cache.isOverBudget(.backend));

    cache.noteResident(.host, 40);
    cache.noteResident(.backend, 25);
    try std.testing.expect(cache.isOverBudget(.host));
    try std.testing.expect(cache.isOverBudget(.backend));

    cache.noteRelease(.host, 40);
    cache.noteRelease(.backend, 25);
    try std.testing.expect(!cache.isOverBudget(.host));
    try std.testing.expect(!cache.isOverBudget(.backend));
}

test "shared cache records denial details" {
    var cache = SharedCache.init(.{
        .host_limit_bytes = 100,
        .backend_limit_bytes = 50,
    });
    cache.noteResident(.host, 90);
    cache.noteDenied(.host, 20);

    var buf: [256]u8 = undefined;
    const msg = try cache.lastDenialString(&buf);
    try std.testing.expect(std.mem.indexOf(u8, msg, "tier=host") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "request=20") != null);
}

test "hard cache budget bounds architecture floors" {
    var cache = SharedCache.init(.{
        .host_limit_bytes = 1024,
        .backend_limit_bytes = 2048,
    });
    cache.configureHardBudget(.{
        .host_limit_bytes = 512,
        .backend_limit_bytes = 768,
    });
    cache.widenToAtLeast(.{
        .host_limit_bytes = 4096,
        .backend_limit_bytes = 8192,
    });
    try std.testing.expectEqual(@as(usize, 512), cache.budget.host_limit_bytes);
    try std.testing.expectEqual(@as(usize, 768), cache.budget.backend_limit_bytes);
}

test "serving cache growth leases only bytes beyond model baseline" {
    var controller = memory.AdmissionController{};
    controller.configureSharedLimits(.{ .host_limit_bytes = 120 });
    defer controller.deinit();

    const limits = memory.Limits{
        .host_limit_bytes = 120,
        .combined_limit_bytes = 120,
    };
    const resident = memory.AdmissionAmounts{ .host_weight_bytes = 100 };
    var model_lease = try controller.tryAcquire(.cpu, limits, resident, false);
    defer model_lease.release();
    try model_lease.retain(resident);

    var cache = SharedCache.init(.{ .host_limit_bytes = 1024 });
    try cache.configureAdmission(
        std.testing.allocator,
        &controller,
        .cpu,
        limits,
        .{ .host_limit_bytes = 100 },
    );
    defer cache.deinitAdmission();

    try cache.reserve(.host, 80);
    try std.testing.expectEqual(@as(usize, 100), controller.snapshot().host_weight_bytes);

    try cache.reserve(.host, 30);
    try std.testing.expectEqual(@as(usize, 110), controller.snapshot().host_weight_bytes);

    // The request fits the configured policy in isolation, but not alongside
    // the currently resident model and cache credit, so callers may retry it
    // after eviction or another lease release.
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, cache.reserve(.host, 20));
    try std.testing.expectEqual(@as(usize, 110), cache.host_bytes);
    try std.testing.expectEqual(@as(usize, 110), controller.snapshot().host_weight_bytes);

    cache.noteRelease(.host, 30);
    try std.testing.expectEqual(@as(usize, 80), cache.host_bytes);
    try std.testing.expectEqual(@as(usize, 100), controller.snapshot().host_weight_bytes);
    cache.noteRelease(.host, 80);
}

test "serving cache preserves retryable live-pressure denial" {
    var controller = memory.AdmissionController{};
    controller.configureSharedLimits(.{ .host_limit_bytes = 1024 });
    // Two bytes is deliberately tiny but still leaves a non-zero safety
    // reserve on every supported live-memory policy (including macOS).
    controller.configureProcessMemoryLimit(2, .explicit);
    defer controller.deinit();

    const limits = memory.Limits{
        .host_limit_bytes = 1024,
        .combined_limit_bytes = 1024,
    };
    var cache = SharedCache.init(.{ .host_limit_bytes = 1024 });
    try cache.configureAdmission(
        std.testing.allocator,
        &controller,
        .cpu,
        limits,
        .{},
    );
    defer cache.deinitAdmission();

    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        cache.reserve(.host, 2),
    );
    try std.testing.expectEqual(@as(usize, 0), cache.host_bytes);
    try std.testing.expectEqual(
        memory.AdmissionAmounts{},
        controller.snapshot(),
    );
}
