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
const builtin = @import("builtin");
const platform = @import("antfly_platform");
const planner = @import("planner.zig");
const kv_pool = @import("../kv/pool.zig");
const gpt_mod = @import("../../models/gpt.zig");

const macos = if (builtin.os.tag == .macos) struct {
    pub const kern_return_t = c_int;
    pub const integer_t = c_int;
    pub const natural_t = c_uint;
    pub const mach_msg_type_number_t = natural_t;
    pub const mach_port_t = c_uint;
    pub const host_t = mach_port_t;
    pub const host_flavor_t = integer_t;
    pub const vm_size_t = usize;
    pub const host_info64_t = [*]integer_t;

    pub const KERN_SUCCESS: kern_return_t = 0;
    pub const HOST_VM_INFO64: host_flavor_t = 4;

    // Avoid @cImport("mach/mach.h") here because Zig 0.16-dev can mis-translate
    // some generated Mach bindings on macOS. The probe only needs this narrow ABI.
    pub const vm_statistics64_data_t = extern struct {
        free_count: natural_t,
        active_count: natural_t,
        inactive_count: natural_t,
        wire_count: natural_t,
        zero_fill_count: u64,
        reactivations: u64,
        pageins: u64,
        pageouts: u64,
        faults: u64,
        cow_faults: u64,
        lookups: u64,
        hits: u64,
        purges: u64,
        purgeable_count: natural_t,
        speculative_count: natural_t,
        decompressions: u64,
        compressions: u64,
        swapins: u64,
        swapouts: u64,
        compressor_page_count: natural_t,
        throttled_count: natural_t,
        external_page_count: natural_t,
        internal_page_count: natural_t,
        total_uncompressed_pages_in_compressor: u64,
    };

    pub const HOST_VM_INFO64_COUNT: mach_msg_type_number_t =
        @as(mach_msg_type_number_t, @intCast(@sizeOf(vm_statistics64_data_t) / @sizeOf(integer_t)));

    pub extern fn sysctlbyname(
        name: [*:0]const u8,
        oldp: ?*anyopaque,
        oldlenp: *usize,
        newp: ?*anyopaque,
        newlen: usize,
    ) c_int;
    pub extern fn mach_host_self() mach_port_t;
    pub extern fn host_page_size(host: host_t, out_page_size: *vm_size_t) kern_return_t;
    pub extern fn host_statistics64(
        host: host_t,
        flavor: host_flavor_t,
        host_info_out: host_info64_t,
        host_info_out_cnt: *mach_msg_type_number_t,
    ) kern_return_t;
} else struct {};

pub const ResidencyTier = planner.ResidencyTier;
pub const BackendClass = planner.BackendClass;

pub const Limits = struct {
    host_limit_bytes: usize = 0,
    backend_limit_bytes: usize = 0,
    combined_limit_bytes: usize = 0,
    kv_limit_bytes: usize = 0,
    scratch_limit_bytes: usize = 0,
};

pub const SystemMemoryInfo = struct {
    total_bytes: usize,
    available_bytes: ?usize = null,
};

pub const ReservationKind = enum {
    weight,
    kv,
    scratch,
};

pub const Reservation = struct {
    kind: ReservationKind,
    tier: ResidencyTier,
    bytes: usize,
};

pub const DenialLimit = enum {
    host_total,
    backend_total,
    combined_total,
    kv_total,
    scratch_total,
    shared_cache_host,
    shared_cache_backend,
};

pub const Denial = struct {
    reservation: Reservation,
    limit: DenialLimit,
    current_bytes: usize,
    requested_total_bytes: usize,
    limit_bytes: usize,
    host_total_bytes: usize,
    backend_total_bytes: usize,
    kv_total_bytes: usize,
    scratch_total_bytes: usize,
};

pub const Estimate = struct {
    prompt_tokens: usize,
    retained_tokens: usize,
    kv_bytes: usize,
    kv_tier: ResidencyTier,
    scratch_bytes: usize,
    scratch_tier: ResidencyTier,
};

pub const EstimateError = error{
    InvalidModelConfig,
    ResourceLimitExceeded,
};

pub const RunBudget = struct {
    limits: Limits,
    host_weight_bytes: usize = 0,
    backend_weight_bytes: usize = 0,
    host_kv_bytes: usize = 0,
    backend_kv_bytes: usize = 0,
    host_scratch_bytes: usize = 0,
    backend_scratch_bytes: usize = 0,
    denials: u64 = 0,
    last_denial: ?Denial = null,
    peak_host_total_bytes: usize = 0,
    peak_backend_total_bytes: usize = 0,

    pub fn init(limits: Limits) RunBudget {
        return .{ .limits = limits };
    }

    pub fn reserveEstimate(self: *RunBudget, estimate: Estimate) !void {
        try self.tryReserve(.{ .kind = .kv, .tier = estimate.kv_tier, .bytes = estimate.kv_bytes });
        errdefer self.release(.{ .kind = .kv, .tier = estimate.kv_tier, .bytes = estimate.kv_bytes });
        try self.tryReserve(.{ .kind = .scratch, .tier = estimate.scratch_tier, .bytes = estimate.scratch_bytes });
    }

    /// Roll back a previously successful reserveEstimate call. Batch admission
    /// uses this when the process-wide controller rejects an otherwise valid
    /// per-run reservation, so one rejected request cannot consume the local
    /// budget of later requests in the same batch.
    pub fn releaseEstimate(self: *RunBudget, estimate: Estimate) void {
        self.release(.{ .kind = .scratch, .tier = estimate.scratch_tier, .bytes = estimate.scratch_bytes });
        self.release(.{ .kind = .kv, .tier = estimate.kv_tier, .bytes = estimate.kv_bytes });
    }

    pub fn tryReserveWeight(self: *RunBudget, tier: ResidencyTier, bytes: usize) !Reservation {
        const reservation = Reservation{
            .kind = .weight,
            .tier = tier,
            .bytes = bytes,
        };
        try self.tryReserve(reservation);
        return reservation;
    }

    pub fn release(self: *RunBudget, reservation: Reservation) void {
        if (reservation.bytes == 0 or reservation.tier == .disk) return;
        switch (reservation.kind) {
            .weight => switch (reservation.tier) {
                .disk => {},
                .host => self.host_weight_bytes -|= reservation.bytes,
                .backend => self.backend_weight_bytes -|= reservation.bytes,
            },
            .kv => switch (reservation.tier) {
                .disk => {},
                .host => self.host_kv_bytes -|= reservation.bytes,
                .backend => self.backend_kv_bytes -|= reservation.bytes,
            },
            .scratch => switch (reservation.tier) {
                .disk => {},
                .host => self.host_scratch_bytes -|= reservation.bytes,
                .backend => self.backend_scratch_bytes -|= reservation.bytes,
            },
        }
    }

    pub fn hostTotalBytes(self: *const RunBudget) usize {
        return addSaturating(addSaturating(self.host_weight_bytes, self.host_kv_bytes), self.host_scratch_bytes);
    }

    pub fn backendTotalBytes(self: *const RunBudget) usize {
        return addSaturating(addSaturating(self.backend_weight_bytes, self.backend_kv_bytes), self.backend_scratch_bytes);
    }

    pub fn kvTotalBytes(self: *const RunBudget) usize {
        return addSaturating(self.host_kv_bytes, self.backend_kv_bytes);
    }

    pub fn scratchTotalBytes(self: *const RunBudget) usize {
        return addSaturating(self.host_scratch_bytes, self.backend_scratch_bytes);
    }

    pub fn noteSharedCacheDenial(
        self: *RunBudget,
        tier: ResidencyTier,
        bytes: usize,
        current_bytes: usize,
        limit_bytes: usize,
    ) void {
        if (tier == .disk) return;
        self.recordDenial(
            switch (tier) {
                .disk => unreachable,
                .host => .shared_cache_host,
                .backend => .shared_cache_backend,
            },
            .{ .kind = .weight, .tier = tier, .bytes = bytes },
            current_bytes,
            addSaturating(current_bytes, bytes),
            limit_bytes,
        );
    }

    pub fn hasLastDenial(self: *const RunBudget) bool {
        return self.last_denial != null;
    }

    pub fn lastDenialString(self: *const RunBudget, buf: []u8) ![]const u8 {
        const denial = self.last_denial orelse {
            return std.fmt.bufPrint(buf, "memory budget exceeded", .{});
        };
        return std.fmt.bufPrint(
            buf,
            "memory budget exceeded: limit={s} reservation={s}/{s} current={d} request={d} next={d} limit={d} totals(host={d} backend={d} kv={d} scratch={d})",
            .{
                @tagName(denial.limit),
                @tagName(denial.reservation.kind),
                @tagName(denial.reservation.tier),
                denial.current_bytes,
                denial.reservation.bytes,
                denial.requested_total_bytes,
                denial.limit_bytes,
                denial.host_total_bytes,
                denial.backend_total_bytes,
                denial.kv_total_bytes,
                denial.scratch_total_bytes,
            },
        );
    }

    fn tryReserve(self: *RunBudget, reservation: Reservation) !void {
        if (reservation.bytes == 0 or reservation.tier == .disk) return;

        const current_host = self.hostTotalBytes();
        const current_backend = self.backendTotalBytes();
        const current_kv = self.kvTotalBytes();
        const current_scratch = self.scratchTotalBytes();
        const next_host = switch (reservation.tier) {
            .host => std.math.add(usize, current_host, reservation.bytes) catch {
                self.recordDenial(.host_total, reservation, current_host, std.math.maxInt(usize), self.limits.host_limit_bytes);
                return error.MemoryBudgetExceeded;
            },
            else => current_host,
        };
        const next_backend = switch (reservation.tier) {
            .backend => std.math.add(usize, current_backend, reservation.bytes) catch {
                self.recordDenial(.backend_total, reservation, current_backend, std.math.maxInt(usize), self.limits.backend_limit_bytes);
                return error.MemoryBudgetExceeded;
            },
            else => current_backend,
        };
        const next_kv = switch (reservation.kind) {
            .kv => std.math.add(usize, current_kv, reservation.bytes) catch {
                self.recordDenial(.kv_total, reservation, current_kv, std.math.maxInt(usize), self.limits.kv_limit_bytes);
                return error.MemoryBudgetExceeded;
            },
            else => current_kv,
        };
        const next_scratch = switch (reservation.kind) {
            .scratch => std.math.add(usize, current_scratch, reservation.bytes) catch {
                self.recordDenial(.scratch_total, reservation, current_scratch, std.math.maxInt(usize), self.limits.scratch_limit_bytes);
                return error.MemoryBudgetExceeded;
            },
            else => current_scratch,
        };
        const next_combined = std.math.add(usize, next_host, next_backend) catch {
            self.recordDenial(
                .combined_total,
                reservation,
                addSaturating(current_host, current_backend),
                std.math.maxInt(usize),
                self.limits.combined_limit_bytes,
            );
            return error.MemoryBudgetExceeded;
        };

        if (self.limits.host_limit_bytes != 0 and next_host > self.limits.host_limit_bytes) {
            self.recordDenial(.host_total, reservation, self.hostTotalBytes(), next_host, self.limits.host_limit_bytes);
            return error.MemoryBudgetExceeded;
        }
        if (self.limits.backend_limit_bytes != 0 and next_backend > self.limits.backend_limit_bytes) {
            self.recordDenial(.backend_total, reservation, self.backendTotalBytes(), next_backend, self.limits.backend_limit_bytes);
            return error.MemoryBudgetExceeded;
        }
        if (self.limits.combined_limit_bytes != 0 and next_combined > self.limits.combined_limit_bytes) {
            self.recordDenial(.combined_total, reservation, addSaturating(self.hostTotalBytes(), self.backendTotalBytes()), next_combined, self.limits.combined_limit_bytes);
            return error.MemoryBudgetExceeded;
        }
        if (self.limits.kv_limit_bytes != 0 and next_kv > self.limits.kv_limit_bytes) {
            self.recordDenial(.kv_total, reservation, self.kvTotalBytes(), next_kv, self.limits.kv_limit_bytes);
            return error.MemoryBudgetExceeded;
        }
        if (self.limits.scratch_limit_bytes != 0 and next_scratch > self.limits.scratch_limit_bytes) {
            self.recordDenial(.scratch_total, reservation, self.scratchTotalBytes(), next_scratch, self.limits.scratch_limit_bytes);
            return error.MemoryBudgetExceeded;
        }

        switch (reservation.kind) {
            .weight => switch (reservation.tier) {
                .disk => {},
                .host => self.host_weight_bytes += reservation.bytes,
                .backend => self.backend_weight_bytes += reservation.bytes,
            },
            .kv => switch (reservation.tier) {
                .disk => {},
                .host => self.host_kv_bytes += reservation.bytes,
                .backend => self.backend_kv_bytes += reservation.bytes,
            },
            .scratch => switch (reservation.tier) {
                .disk => {},
                .host => self.host_scratch_bytes += reservation.bytes,
                .backend => self.backend_scratch_bytes += reservation.bytes,
            },
        }

        self.peak_host_total_bytes = @max(self.peak_host_total_bytes, self.hostTotalBytes());
        self.peak_backend_total_bytes = @max(self.peak_backend_total_bytes, self.backendTotalBytes());
    }

    fn recordDenial(
        self: *RunBudget,
        limit: DenialLimit,
        reservation: Reservation,
        current_bytes: usize,
        requested_total_bytes: usize,
        limit_bytes: usize,
    ) void {
        self.denials +|= 1;
        self.last_denial = .{
            .reservation = reservation,
            .limit = limit,
            .current_bytes = current_bytes,
            .requested_total_bytes = requested_total_bytes,
            .limit_bytes = limit_bytes,
            .host_total_bytes = self.hostTotalBytes(),
            .backend_total_bytes = self.backendTotalBytes(),
            .kv_total_bytes = self.kvTotalBytes(),
            .scratch_total_bytes = self.scratchTotalBytes(),
        };
    }
};

/// Process-wide resource admission. `RunBudget` protects one execution; this
/// controller accounts for resident models and concurrent executions together.
pub const AdmissionAmounts = struct {
    host_weight_bytes: usize = 0,
    backend_weight_bytes: usize = 0,
    host_kv_bytes: usize = 0,
    backend_kv_bytes: usize = 0,
    host_scratch_bytes: usize = 0,
    backend_scratch_bytes: usize = 0,

    pub fn hostTotalBytes(self: @This()) usize {
        return addSaturating(addSaturating(self.host_weight_bytes, self.host_kv_bytes), self.host_scratch_bytes);
    }

    pub fn backendTotalBytes(self: @This()) usize {
        return addSaturating(addSaturating(self.backend_weight_bytes, self.backend_kv_bytes), self.backend_scratch_bytes);
    }

    pub fn kvTotalBytes(self: @This()) usize {
        return addSaturating(self.host_kv_bytes, self.backend_kv_bytes);
    }

    pub fn scratchTotalBytes(self: @This()) usize {
        return addSaturating(self.host_scratch_bytes, self.backend_scratch_bytes);
    }

    pub fn fromEstimate(estimate: Estimate) @This() {
        var amounts: @This() = .{};
        switch (estimate.kv_tier) {
            .disk => {},
            .host => amounts.host_kv_bytes = estimate.kv_bytes,
            .backend => amounts.backend_kv_bytes = estimate.kv_bytes,
        }
        switch (estimate.scratch_tier) {
            .disk => {},
            .host => amounts.host_scratch_bytes = estimate.scratch_bytes,
            .backend => amounts.backend_scratch_bytes = estimate.scratch_bytes,
        }
        return amounts;
    }

    pub fn merge(self: @This(), other: @This()) !@This() {
        return addAdmissionAmounts(self, other) orelse error.ResourceLimitExceeded;
    }

    fn hostTotalBytesChecked(self: @This()) !usize {
        return std.math.add(
            usize,
            try std.math.add(usize, self.host_weight_bytes, self.host_kv_bytes),
            self.host_scratch_bytes,
        );
    }

    fn backendTotalBytesChecked(self: @This()) !usize {
        return std.math.add(
            usize,
            try std.math.add(usize, self.backend_weight_bytes, self.backend_kv_bytes),
            self.backend_scratch_bytes,
        );
    }

    fn kvTotalBytesChecked(self: @This()) !usize {
        return std.math.add(usize, self.host_kv_bytes, self.backend_kv_bytes);
    }

    fn scratchTotalBytesChecked(self: @This()) !usize {
        return std.math.add(usize, self.host_scratch_bytes, self.backend_scratch_bytes);
    }
};

pub const AdmissionLease = struct {
    controller: ?*AdmissionController,
    amounts: AdmissionAmounts,

    pub fn release(self: *AdmissionLease) void {
        const controller = self.controller orelse return;
        controller.release(self.amounts);
        self.controller = null;
        self.amounts = .{};
    }
};

pub const AdmissionController = struct {
    mutex: std.atomic.Mutex = .unlocked,
    admitted: AdmissionAmounts = .{},

    pub fn tryAcquire(
        self: *AdmissionController,
        limits: Limits,
        amounts: AdmissionAmounts,
        check_live_memory: bool,
    ) !AdmissionLease {
        spinLockAdmission(&self.mutex);
        defer self.mutex.unlock();

        const next = addAdmissionAmounts(self.admitted, amounts) orelse
            return error.ResourceLimitExceeded;
        const request_host = amounts.hostTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const request_backend = amounts.backendTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const request_combined = std.math.add(usize, request_host, request_backend) catch
            return error.ResourceLimitExceeded;
        const request_kv = amounts.kvTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const request_scratch = amounts.scratchTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const next_host = next.hostTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const next_backend = next.backendTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const next_combined = std.math.add(usize, next_host, next_backend) catch
            return error.ResourceLimitExceeded;
        const next_kv = next.kvTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const next_scratch = next.scratchTotalBytesChecked() catch return error.ResourceLimitExceeded;

        try checkAdmissionLimit(request_host, next_host, limits.host_limit_bytes);
        try checkAdmissionLimit(request_backend, next_backend, limits.backend_limit_bytes);
        try checkAdmissionLimit(request_combined, next_combined, limits.combined_limit_bytes);
        try checkAdmissionLimit(request_kv, next_kv, limits.kv_limit_bytes);
        try checkAdmissionLimit(request_scratch, next_scratch, limits.scratch_limit_bytes);

        if (check_live_memory) {
            // Metal allocations consume unified system memory. CUDA allocations are
            // accounted against the backend budget and must not also be charged to
            // Linux MemAvailable, or a valid device-resident model is rejected merely
            // because its VRAM footprint exceeds free host RAM.
            const live_host_incremental = if (builtin.os.tag == .macos)
                request_combined
            else
                request_host;
            try checkLiveHostMemory(live_host_incremental);
        }
        self.admitted = next;
        return .{ .controller = self, .amounts = amounts };
    }

    pub fn snapshot(self: *AdmissionController) AdmissionAmounts {
        spinLockAdmission(&self.mutex);
        defer self.mutex.unlock();
        return self.admitted;
    }

    fn release(self: *AdmissionController, amounts: AdmissionAmounts) void {
        spinLockAdmission(&self.mutex);
        defer self.mutex.unlock();
        self.admitted.host_weight_bytes -|= amounts.host_weight_bytes;
        self.admitted.backend_weight_bytes -|= amounts.backend_weight_bytes;
        self.admitted.host_kv_bytes -|= amounts.host_kv_bytes;
        self.admitted.backend_kv_bytes -|= amounts.backend_kv_bytes;
        self.admitted.host_scratch_bytes -|= amounts.host_scratch_bytes;
        self.admitted.backend_scratch_bytes -|= amounts.backend_scratch_bytes;
    }
};

fn spinLockAdmission(mutex: *std.atomic.Mutex) void {
    platform.sync.lockYielding(mutex);
}

fn addAdmissionAmounts(a: AdmissionAmounts, b: AdmissionAmounts) ?AdmissionAmounts {
    return .{
        .host_weight_bytes = std.math.add(usize, a.host_weight_bytes, b.host_weight_bytes) catch return null,
        .backend_weight_bytes = std.math.add(usize, a.backend_weight_bytes, b.backend_weight_bytes) catch return null,
        .host_kv_bytes = std.math.add(usize, a.host_kv_bytes, b.host_kv_bytes) catch return null,
        .backend_kv_bytes = std.math.add(usize, a.backend_kv_bytes, b.backend_kv_bytes) catch return null,
        .host_scratch_bytes = std.math.add(usize, a.host_scratch_bytes, b.host_scratch_bytes) catch return null,
        .backend_scratch_bytes = std.math.add(usize, a.backend_scratch_bytes, b.backend_scratch_bytes) catch return null,
    };
}

fn addSaturating(a: usize, b: usize) usize {
    return std.math.add(usize, a, b) catch std.math.maxInt(usize);
}

fn checkAdmissionLimit(request: usize, next: usize, limit: usize) !void {
    if (limit == 0 or next <= limit) return;
    if (request > limit) return error.ResourceLimitExceeded;
    return error.ResourceTemporarilyUnavailable;
}

fn checkLiveHostMemory(incremental_bytes: usize) !void {
    const info = currentSystemMemoryInfo() orelse return;
    return checkLiveHostMemoryWithInfo(info, incremental_bytes);
}

/// Preserve the release default on large hosts while guaranteeing that a
/// container or smaller machine can use at least half of its effective memory.
/// A fixed multi-GiB floor cannot be applied after cgroup constraints: when the
/// effective total is below that floor, every positive admission is impossible.
fn liveHostMemoryHeadroom(total_bytes: usize) usize {
    const preferred = clampBytes(@max(total_bytes / 4, gib(6)), gib(4), gib(24));
    return @min(preferred, total_bytes / 2);
}

fn checkLiveHostMemoryWithInfo(info: SystemMemoryInfo, incremental_bytes: usize) !void {
    const available = info.available_bytes orelse return;
    const headroom = liveHostMemoryHeadroom(info.total_bytes);
    const required = std.math.add(usize, incremental_bytes, headroom) catch
        return error.ResourceLimitExceeded;
    if (available < required) return error.ResourceTemporarilyUnavailable;
}

pub fn defaultLimitsForBackend(backend: BackendClass) Limits {
    if (currentSystemMemoryInfo()) |info| {
        // Admission policy is stable for the lifetime of a machine configuration.
        // Current pressure is checked separately immediately before allocation.
        return deriveLimitsForBackend(backend, .{
            .total_bytes = info.total_bytes,
            .available_bytes = info.total_bytes,
        });
    }
    return staticLimitsForBackend(backend);
}

fn staticLimitsForBackend(backend: BackendClass) Limits {
    return switch (backend) {
        .cpu => .{
            .host_limit_bytes = 2 * 1024 * 1024 * 1024,
            .backend_limit_bytes = 0,
            .combined_limit_bytes = 2 * 1024 * 1024 * 1024,
            .kv_limit_bytes = 768 * 1024 * 1024,
            .scratch_limit_bytes = 256 * 1024 * 1024,
        },
        .gpu => .{
            .host_limit_bytes = 2 * 1024 * 1024 * 1024,
            .backend_limit_bytes = 6 * 1024 * 1024 * 1024,
            .combined_limit_bytes = 8 * 1024 * 1024 * 1024,
            .kv_limit_bytes = 1024 * 1024 * 1024,
            .scratch_limit_bytes = 512 * 1024 * 1024,
        },
    };
}

pub fn currentSystemMemoryInfo() ?SystemMemoryInfo {
    return switch (builtin.os.tag) {
        .macos => probeSystemMemoryInfoMacos(),
        .linux => probeSystemMemoryInfoLinux(),
        else => null,
    };
}

fn deriveLimitsForBackend(backend: BackendClass, info: SystemMemoryInfo) Limits {
    const total = info.total_bytes;
    const available = info.available_bytes orelse total;
    const reserve_headroom = liveHostMemoryHeadroom(total);
    const safe_pool = available -| @min(available, reserve_headroom);

    return switch (backend) {
        .cpu => blk: {
            // Never let minimum tuning floors exceed the memory left after
            // headroom. The live pressure check sees only materialized bytes;
            // this stable cap also protects concurrent reservations that have
            // not reached memory.current/MemAvailable yet.
            const host_limit = @min(
                clampBytes(safe_pool / 2, gib(2), gib(8)),
                safe_pool,
            );
            break :blk .{
                .host_limit_bytes = host_limit,
                .backend_limit_bytes = 0,
                .combined_limit_bytes = host_limit,
                .kv_limit_bytes = @min(
                    clampBytes(safe_pool / 6, mib(512), gib(2)),
                    host_limit,
                ),
                .scratch_limit_bytes = @min(
                    clampBytes(safe_pool / 12, mib(256), gib(1)),
                    host_limit,
                ),
            };
        },
        .gpu => blk: {
            var combined = clampBytes(safe_pool / 2, gib(6), gib(12));
            // Metal uses unified memory; discrete Linux GPU residency is not
            // constrained by the process cgroup's host-memory limit.
            if (builtin.os.tag == .macos) combined = @min(combined, safe_pool);
            const host_limit = @min(
                clampBytes(combined / 4, gib(1), gib(3)),
                safe_pool,
            );
            break :blk .{
                .host_limit_bytes = host_limit,
                .backend_limit_bytes = @min(
                    clampBytes((combined * 3) / 4, gib(4), gib(9)),
                    combined,
                ),
                .combined_limit_bytes = combined,
                .kv_limit_bytes = @min(
                    clampBytes(combined / 4, mib(768), gib(3)),
                    combined,
                ),
                .scratch_limit_bytes = @min(
                    clampBytes(combined / 8, mib(384), gib(2)),
                    combined,
                ),
            };
        },
    };
}

fn probeSystemMemoryInfoMacos() ?SystemMemoryInfo {
    if (builtin.os.tag != .macos) return null;

    var total_raw: u64 = 0;
    var total_len: usize = @sizeOf(u64);
    if (macos.sysctlbyname("hw.memsize", @ptrCast(&total_raw), &total_len, null, 0) != 0 or total_raw == 0) return null;

    var page_size: macos.vm_size_t = 0;
    if (macos.host_page_size(macos.mach_host_self(), &page_size) != macos.KERN_SUCCESS or page_size == 0) {
        return .{ .total_bytes = @intCast(total_raw), .available_bytes = null };
    }

    var vm_stats: macos.vm_statistics64_data_t = undefined;
    var count: macos.mach_msg_type_number_t = macos.HOST_VM_INFO64_COUNT;
    if (macos.host_statistics64(
        macos.mach_host_self(),
        macos.HOST_VM_INFO64,
        @ptrCast(&vm_stats),
        &count,
    ) != macos.KERN_SUCCESS) {
        return .{ .total_bytes = @intCast(total_raw), .available_bytes = null };
    }

    const available_pages: u64 =
        @as(u64, @intCast(vm_stats.free_count)) +
        @as(u64, @intCast(vm_stats.inactive_count)) +
        @as(u64, @intCast(vm_stats.speculative_count));
    const available_bytes_u64 = available_pages * @as(u64, @intCast(page_size));
    return .{
        .total_bytes = @intCast(total_raw),
        .available_bytes = @intCast(@min(available_bytes_u64, total_raw)),
    };
}

const LinuxMemInfoFields = struct {
    total_kib: u64 = 0,
    available_kib: ?u64 = null,
    free_kib: u64 = 0,
    buffers_kib: u64 = 0,
    cached_kib: u64 = 0,
    reclaimable_kib: u64 = 0,
    shmem_kib: u64 = 0,
};

const CgroupMemoryInfo = struct {
    limit_bytes: ?usize = null,
    current_bytes: ?usize = null,
    available_bytes: ?usize = null,
};

const CgroupHierarchyProbe = struct {
    info: CgroupMemoryInfo = .{},
    /// True when the initially addressed process directory contained at least
    /// one memory controller file. A finite ancestor alone is insufficient:
    /// with a subtree bind mount it can be the mount root reached through an
    /// incorrectly untrimmed host-side process path.
    leaf_present: bool = false,
};

const CgroupPaths = struct {
    v2: ?[]const u8 = null,
    v1_memory: ?[]const u8 = null,
};

const CgroupMount = struct {
    root_storage: [std.fs.max_path_bytes]u8 = undefined,
    root_len: usize = 0,
    mount_point_storage: [std.fs.max_path_bytes]u8 = undefined,
    mount_point_len: usize = 0,

    fn root(self: *const CgroupMount) []const u8 {
        return self.root_storage[0..self.root_len];
    }

    fn mountPoint(self: *const CgroupMount) []const u8 {
        return self.mount_point_storage[0..self.mount_point_len];
    }

    fn valid(self: *const CgroupMount) bool {
        return self.root_len > 0 and self.mount_point_len > 0;
    }
};

const CgroupMounts = struct {
    v2: CgroupMount = .{},
    v1_memory: CgroupMount = .{},
};

fn readSmallLinuxFile(path: []const u8, buffer: []u8) ?[]const u8 {
    if (builtin.os.tag != .linux or buffer.len == 0) return null;
    const fd = std.posix.openat(
        std.posix.AT.FDCWD,
        path,
        .{ .ACCMODE = .RDONLY, .CLOEXEC = true },
        0,
    ) catch return null;
    defer _ = std.posix.system.close(fd);

    var used: usize = 0;
    while (used < buffer.len) {
        const count = std.posix.read(fd, buffer[used..]) catch return null;
        if (count == 0) break;
        used += count;
    }
    if (used == 0) return null;
    return buffer[0..used];
}

fn parseLinuxMemInfo(bytes: []const u8) ?SystemMemoryInfo {
    var fields = LinuxMemInfoFields{};
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const key = line[0..colon];
        var values = std.mem.tokenizeAny(u8, line[colon + 1 ..], " \t");
        const raw = values.next() orelse continue;
        const value = std.fmt.parseUnsigned(u64, raw, 10) catch continue;
        if (std.mem.eql(u8, key, "MemTotal")) {
            fields.total_kib = value;
        } else if (std.mem.eql(u8, key, "MemAvailable")) {
            fields.available_kib = value;
        } else if (std.mem.eql(u8, key, "MemFree")) {
            fields.free_kib = value;
        } else if (std.mem.eql(u8, key, "Buffers")) {
            fields.buffers_kib = value;
        } else if (std.mem.eql(u8, key, "Cached")) {
            fields.cached_kib = value;
        } else if (std.mem.eql(u8, key, "SReclaimable")) {
            fields.reclaimable_kib = value;
        } else if (std.mem.eql(u8, key, "Shmem")) {
            fields.shmem_kib = value;
        }
    }
    if (fields.total_kib == 0) return null;

    const available_kib = fields.available_kib orelse blk: {
        var fallback = std.math.add(u64, fields.free_kib, fields.buffers_kib) catch
            std.math.maxInt(u64);
        fallback = std.math.add(u64, fallback, fields.cached_kib) catch
            std.math.maxInt(u64);
        fallback = std.math.add(u64, fallback, fields.reclaimable_kib) catch
            std.math.maxInt(u64);
        break :blk fallback -| fields.shmem_kib;
    };
    const total_bytes_u64 = std.math.mul(u64, fields.total_kib, 1024) catch return null;
    const available_bytes_u64 = std.math.mul(u64, available_kib, 1024) catch
        std.math.maxInt(u64);
    return .{
        .total_bytes = std.math.cast(usize, total_bytes_u64) orelse return null,
        .available_bytes = std.math.cast(
            usize,
            @min(available_bytes_u64, total_bytes_u64),
        ),
    };
}

fn controllerListContains(controllers: []const u8, expected: []const u8) bool {
    var it = std.mem.splitScalar(u8, controllers, ',');
    while (it.next()) |controller| {
        if (std.mem.eql(u8, controller, expected)) return true;
    }
    return false;
}

fn parseCgroupPaths(bytes: []const u8) CgroupPaths {
    var result = CgroupPaths{};
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |line| {
        const first = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const second = std.mem.indexOfScalarPos(u8, line, first + 1, ':') orelse continue;
        const hierarchy = line[0..first];
        const controllers = line[first + 1 .. second];
        const path = line[second + 1 ..];
        if (path.len == 0 or path[0] != '/' or std.mem.indexOf(u8, path, "..") != null) continue;
        if (std.mem.eql(u8, hierarchy, "0") and controllers.len == 0) {
            result.v2 = path;
        } else if (controllerListContains(controllers, "memory")) {
            result.v1_memory = path;
        }
    }
    return result;
}

fn readLinuxUnsignedFile(path: []const u8) ?usize {
    var buffer: [128]u8 = undefined;
    const bytes = readSmallLinuxFile(path, &buffer) orelse return null;
    const raw = std.mem.trim(u8, bytes, " \t\r\n");
    if (raw.len == 0 or std.mem.eql(u8, raw, "max")) return null;
    return std.fmt.parseUnsigned(usize, raw, 10) catch null;
}

fn decodeMountInfoPath(destination: []u8, encoded: []const u8) ?usize {
    if (encoded.len == 0 or encoded[0] != '/') return null;
    var source_index: usize = 0;
    var destination_index: usize = 0;
    while (source_index < encoded.len) {
        if (destination_index == destination.len) return null;
        if (encoded[source_index] == '\\' and source_index + 3 < encoded.len) {
            const a = encoded[source_index + 1];
            const b = encoded[source_index + 2];
            const c = encoded[source_index + 3];
            if (a >= '0' and a <= '7' and
                b >= '0' and b <= '7' and
                c >= '0' and c <= '7')
            {
                const decoded_value =
                    @as(u16, a - '0') * 64 +
                    @as(u16, b - '0') * 8 +
                    @as(u16, c - '0');
                if (decoded_value > std.math.maxInt(u8)) return null;
                destination[destination_index] = @intCast(decoded_value);
                source_index += 4;
                destination_index += 1;
                continue;
            }
        }
        destination[destination_index] = encoded[source_index];
        source_index += 1;
        destination_index += 1;
    }
    const decoded = destination[0..destination_index];
    var components = std.mem.splitScalar(u8, decoded, '/');
    while (components.next()) |component| {
        if (std.mem.eql(u8, component, "..")) return null;
    }
    return destination_index;
}

fn parseCgroupMountInfoLine(line: []const u8, mounts: *CgroupMounts) void {
    const separator = std.mem.indexOf(u8, line, " - ") orelse return;
    var before = std.mem.tokenizeScalar(u8, line[0..separator], ' ');
    _ = before.next() orelse return;
    _ = before.next() orelse return;
    _ = before.next() orelse return;
    const encoded_root = before.next() orelse return;
    const encoded_mount_point = before.next() orelse return;

    var after = std.mem.tokenizeScalar(u8, line[separator + 3 ..], ' ');
    const filesystem_type = after.next() orelse return;
    _ = after.next() orelse return;
    const super_options = after.next() orelse "";
    const is_v2 = std.mem.eql(u8, filesystem_type, "cgroup2");
    const is_v1_memory = std.mem.eql(u8, filesystem_type, "cgroup") and
        controllerListContains(super_options, "memory");
    if (!is_v2 and !is_v1_memory) return;

    const mount = if (is_v2) &mounts.v2 else &mounts.v1_memory;
    const root_len = decodeMountInfoPath(&mount.root_storage, encoded_root) orelse return;
    const mount_point_len = decodeMountInfoPath(
        &mount.mount_point_storage,
        encoded_mount_point,
    ) orelse return;
    mount.root_len = root_len;
    mount.mount_point_len = mount_point_len;
}

fn probeCgroupMountsLinux() CgroupMounts {
    var mounts = CgroupMounts{};
    if (builtin.os.tag != .linux) return mounts;
    const fd = std.posix.openat(
        std.posix.AT.FDCWD,
        "/proc/self/mountinfo",
        .{ .ACCMODE = .RDONLY, .CLOEXEC = true },
        0,
    ) catch return mounts;
    defer _ = std.posix.system.close(fd);

    var read_buffer: [4096]u8 = undefined;
    var line_buffer: [8192]u8 = undefined;
    var line_len: usize = 0;
    var discard_line = false;
    while (true) {
        const count = std.posix.read(fd, read_buffer[0..]) catch return mounts;
        if (count == 0) break;
        for (read_buffer[0..count]) |byte| {
            if (byte == '\n') {
                if (!discard_line and line_len > 0)
                    parseCgroupMountInfoLine(line_buffer[0..line_len], &mounts);
                line_len = 0;
                discard_line = false;
                continue;
            }
            if (discard_line) continue;
            if (line_len == line_buffer.len) {
                line_len = 0;
                discard_line = true;
                continue;
            }
            line_buffer[line_len] = byte;
            line_len += 1;
        }
    }
    if (!discard_line and line_len > 0)
        parseCgroupMountInfoLine(line_buffer[0..line_len], &mounts);
    return mounts;
}

fn cgroupPathRelativeToMount(
    process_path: []const u8,
    mount_root: []const u8,
) ?[]const u8 {
    if (process_path.len == 0 or process_path[0] != '/' or
        mount_root.len == 0 or mount_root[0] != '/')
    {
        return null;
    }
    // In a cgroup namespace the process path is commonly "/" even when
    // mountinfo reports the host-side subtree as the mount root.
    if (std.mem.eql(u8, process_path, "/")) return process_path;
    if (std.mem.eql(u8, mount_root, "/")) return process_path;
    if (std.mem.eql(u8, process_path, mount_root)) return "/";
    if (std.mem.startsWith(u8, process_path, mount_root) and
        process_path.len > mount_root.len and
        process_path[mount_root.len] == '/')
    {
        return process_path[mount_root.len..];
    }
    // Namespace-relative cgroup paths do not necessarily share the host-side
    // mount root prefix. Treat them as relative to the visible mount.
    return process_path;
}

fn cgroupDirectoryPath(
    buffer: []u8,
    mount_point: []const u8,
    relative: []const u8,
) ?[]u8 {
    if (mount_point.len == 0 or mount_point[0] != '/' or
        relative.len == 0 or relative[0] != '/')
    {
        return null;
    }
    if (std.mem.eql(u8, mount_point, "/")) {
        return std.fmt.bufPrint(buffer, "{s}", .{relative}) catch null;
    }
    const trimmed_mount = std.mem.trimEnd(u8, mount_point, "/");
    return if (std.mem.eql(u8, relative, "/"))
        std.fmt.bufPrint(buffer, "{s}", .{trimmed_mount}) catch null
    else
        std.fmt.bufPrint(buffer, "{s}{s}", .{ trimmed_mount, relative }) catch null;
}

fn accumulateCgroupLevel(
    result: *CgroupMemoryInfo,
    limit: ?usize,
    current: ?usize,
) void {
    const finite_limit = limit orelse return;
    // cgroup v1 uses a near-max integer as its unlimited sentinel.
    if (finite_limit >= std.math.maxInt(usize) / 2) return;
    if (result.limit_bytes == null or finite_limit < result.limit_bytes.?) {
        result.limit_bytes = finite_limit;
        result.current_bytes = current;
    }
    if (current) |usage| {
        const available = finite_limit -| @min(usage, finite_limit);
        result.available_bytes = if (result.available_bytes) |existing|
            @min(existing, available)
        else
            available;
    }
}

fn readCgroupHierarchy(
    mount_point: []const u8,
    mount_root: []const u8,
    process_path: []const u8,
    limit_filename: []const u8,
    current_filename: []const u8,
) CgroupHierarchyProbe {
    const relative = cgroupPathRelativeToMount(process_path, mount_root) orelse return .{};
    var directory_buffer: [std.fs.max_path_bytes]u8 = undefined;
    var directory = cgroupDirectoryPath(
        &directory_buffer,
        mount_point,
        relative,
    ) orelse return .{};
    const leaf_directory_len = directory.len;
    const hierarchy_root_len = if (std.mem.eql(u8, mount_point, "/"))
        @as(usize, 1)
    else
        std.mem.trimEnd(u8, mount_point, "/").len;

    var result = CgroupHierarchyProbe{};
    var limit_path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    var current_path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    while (directory.len >= hierarchy_root_len) {
        const limit_path = std.fmt.bufPrint(
            &limit_path_buffer,
            "{s}/{s}",
            .{ directory, limit_filename },
        ) catch break;
        const current_path = std.fmt.bufPrint(
            &current_path_buffer,
            "{s}/{s}",
            .{ directory, current_filename },
        ) catch break;
        const limit = readLinuxUnsignedFile(limit_path);
        const current = readLinuxUnsignedFile(current_path);
        if (directory.len == leaf_directory_len) {
            result.leaf_present = limit != null or current != null;
        }
        accumulateCgroupLevel(&result.info, limit, current);
        if (directory.len == hierarchy_root_len) break;
        const parent = std.fs.path.dirname(directory) orelse break;
        if (parent.len < hierarchy_root_len) break;
        directory = directory_buffer[0..parent.len];
    }
    return result;
}

fn canonicalCgroupProbeIsAuthoritative(probe: CgroupHierarchyProbe) bool {
    return probe.leaf_present and probe.info.limit_bytes != null;
}

fn probeCgroupMemoryInfoLinux() CgroupMemoryInfo {
    var cgroup_buffer: [4096]u8 = undefined;
    const cgroup_bytes = readSmallLinuxFile("/proc/self/cgroup", &cgroup_buffer) orelse
        return .{};
    const paths = parseCgroupPaths(cgroup_bytes);
    var canonical_fallback = CgroupMemoryInfo{};
    if (paths.v2) |path| {
        const probe = readCgroupHierarchy(
            "/sys/fs/cgroup",
            "/",
            path,
            "memory.max",
            "memory.current",
        );
        if (canonicalCgroupProbeIsAuthoritative(probe)) return probe.info;
        if (probe.info.limit_bytes != null) canonical_fallback = probe.info;
    }
    if (paths.v1_memory) |path| {
        const probe = readCgroupHierarchy(
            "/sys/fs/cgroup/memory",
            "/",
            path,
            "memory.limit_in_bytes",
            "memory.usage_in_bytes",
        );
        if (canonicalCgroupProbeIsAuthoritative(probe)) return probe.info;
        if (canonical_fallback.limit_bytes == null and probe.info.limit_bytes != null)
            canonical_fallback = probe.info;
    }

    // Canonical mounts cover the common container path without reparsing
    // mountinfo on every admission. A finite ancestor is accepted on this fast
    // path only when the addressed leaf itself exists; otherwise a subtree bind
    // mount can make the canonical walk land on a looser mount-root limit.
    const mounts = probeCgroupMountsLinux();
    if (paths.v2) |path| {
        if (mounts.v2.valid()) {
            const probe = readCgroupHierarchy(
                mounts.v2.mountPoint(),
                mounts.v2.root(),
                path,
                "memory.max",
                "memory.current",
            );
            if (probe.info.limit_bytes != null) return probe.info;
        }
    }
    if (paths.v1_memory) |path| {
        if (mounts.v1_memory.valid()) {
            const probe = readCgroupHierarchy(
                mounts.v1_memory.mountPoint(),
                mounts.v1_memory.root(),
                path,
                "memory.limit_in_bytes",
                "memory.usage_in_bytes",
            );
            if (probe.info.limit_bytes != null) return probe.info;
        }
    }
    return canonical_fallback;
}

fn applyCgroupMemoryInfo(
    host: SystemMemoryInfo,
    cgroup: CgroupMemoryInfo,
) SystemMemoryInfo {
    const raw_limit = cgroup.limit_bytes orelse return host;
    // Cgroup v1 represents "unlimited" with a very large numeric sentinel.
    // Ignore any limit above host RAM; applying memory.current to that sentinel
    // would incorrectly subtract process usage from MemAvailable a second time.
    if (raw_limit > host.total_bytes) return host;
    const limit = raw_limit;
    var available = host.available_bytes;
    const hierarchy_available = cgroup.available_bytes orelse if (cgroup.current_bytes) |current|
        limit -| @min(current, limit)
    else
        null;
    if (hierarchy_available) |cgroup_available| {
        available = if (available) |host_available|
            @min(host_available, cgroup_available)
        else
            cgroup_available;
    }
    return .{
        .total_bytes = limit,
        .available_bytes = if (available) |value| @min(value, limit) else null,
    };
}

fn probeSystemMemoryInfoLinux() ?SystemMemoryInfo {
    if (builtin.os.tag != .linux) return null;
    var meminfo_buffer: [8192]u8 = undefined;
    const bytes = readSmallLinuxFile("/proc/meminfo", &meminfo_buffer) orelse return null;
    const host = parseLinuxMemInfo(bytes) orelse return null;
    return applyCgroupMemoryInfo(host, probeCgroupMemoryInfoLinux());
}

fn mib(value: usize) usize {
    return value * 1024 * 1024;
}

fn gib(value: usize) usize {
    return value * 1024 * 1024 * 1024;
}

fn clampBytes(value: usize, min_value: usize, max_value: usize) usize {
    return @min(@max(value, min_value), max_value);
}

pub fn estimateGptGeneration(
    backend: kv_pool.BackendKind,
    kv_dtype: kv_pool.KvDType,
    config: gpt_mod.Config,
    prompt_tokens: usize,
    max_tokens: usize,
    prefill_chunk_size: usize,
) EstimateError!Estimate {
    if (config.num_hidden_layers == 0 or
        config.hidden_size == 0 or
        config.num_attention_heads == 0 or
        config.vocab_size == 0)
    {
        return error.InvalidModelConfig;
    }
    const total_tokens = std.math.add(usize, prompt_tokens, max_tokens) catch
        return error.ResourceLimitExceeded;
    const retained_tokens = blk: {
        if (config.position_encoding != .absolute and config.sliding_window > 0) {
            break :blk @min(total_tokens, @as(usize, @intCast(config.sliding_window)));
        }
        if (config.position_encoding != .absolute and config.max_position_embeddings > 0) {
            break :blk @min(total_tokens, @as(usize, @intCast(config.max_position_embeddings)));
        }
        break :blk total_tokens;
    };
    const page_aligned_tokens = alignForwardChecked(@max(retained_tokens, 1), 16) catch
        return error.ResourceLimitExceeded;
    const max_kv_heads = config.maxKvHeads();
    const max_head_dim = try estimateMaxHeadDim(config);
    if (max_kv_heads == 0 or max_head_dim == 0) return error.InvalidModelConfig;
    const kv_pair_bytes = kv_dtype.bytesForTokenPairChecked(max_kv_heads, max_head_dim) catch
        return error.ResourceLimitExceeded;
    const token_layers = std.math.mul(
        usize,
        page_aligned_tokens,
        @as(usize, config.num_hidden_layers),
    ) catch return error.ResourceLimitExceeded;
    const kv_bytes = std.math.mul(usize, token_layers, kv_pair_bytes) catch
        return error.ResourceLimitExceeded;

    const scratch_rows = @max(prefill_chunk_size, 1);
    const hidden = @as(usize, @intCast(config.hidden_size));
    const heads = @as(usize, @intCast(config.num_attention_heads));
    const head_dim = @as(usize, @intCast(config.headDim()));
    const vocab = @as(usize, @intCast(config.vocab_size));
    const attention_width = std.math.mul(usize, heads, head_dim) catch
        return error.ResourceLimitExceeded;
    const hidden_scratch = checkedProduct(&.{ scratch_rows, hidden, 8, @sizeOf(f32) }) catch
        return error.ResourceLimitExceeded;
    const attn_scratch = checkedProduct(&.{ scratch_rows, @max(attention_width, hidden), 4, @sizeOf(f32) }) catch
        return error.ResourceLimitExceeded;
    const logits_scratch = std.math.mul(usize, vocab, @sizeOf(f32)) catch
        return error.ResourceLimitExceeded;
    const activation_scratch = std.math.add(usize, hidden_scratch, attn_scratch) catch
        return error.ResourceLimitExceeded;
    const scratch_bytes = std.math.add(usize, activation_scratch, logits_scratch) catch
        return error.ResourceLimitExceeded;

    return .{
        .prompt_tokens = prompt_tokens,
        .retained_tokens = retained_tokens,
        .kv_bytes = kv_bytes,
        .kv_tier = switch (backend) {
            .native => .host,
            .metal, .cuda => .backend,
        },
        .scratch_bytes = scratch_bytes,
        .scratch_tier = switch (backend) {
            .native => .host,
            .metal, .cuda => .backend,
        },
    };
}

fn alignForwardChecked(value: usize, alignment: usize) !usize {
    std.debug.assert(std.math.isPowerOfTwo(alignment));
    return (try std.math.add(usize, value, alignment - 1)) & ~(alignment - 1);
}

fn estimateMaxHeadDim(config: gpt_mod.Config) EstimateError!u32 {
    if (config.family != .deepseek_v4) return config.maxHeadDim();

    const base_head_dim = if (config.attention_head_dim > 0)
        config.attention_head_dim
    else
        config.hidden_size / config.num_attention_heads;
    const kv_lora: usize = if (config.deepseek_v4_kv_lora_rank > 0)
        config.deepseek_v4_kv_lora_rank
    else if (base_head_dim > config.deepseek_v4_qk_rope_head_dim)
        base_head_dim - config.deepseek_v4_qk_rope_head_dim
    else
        0;
    const width = std.math.add(usize, kv_lora, config.deepseek_v4_qk_rope_head_dim) catch
        return error.ResourceLimitExceeded;
    if (width > 0) {
        return std.math.cast(u32, width) orelse error.ResourceLimitExceeded;
    }
    const fallback = std.math.mul(
        usize,
        @as(usize, config.effectiveKVHeads()),
        @as(usize, base_head_dim),
    ) catch return error.ResourceLimitExceeded;
    return std.math.cast(u32, fallback) orelse error.ResourceLimitExceeded;
}

fn checkedProduct(values: []const usize) !usize {
    var result: usize = 1;
    for (values) |value| result = try std.math.mul(usize, result, value);
    return result;
}

test "linux meminfo parser uses MemAvailable and converts KiB" {
    const info = parseLinuxMemInfo(
        \\MemTotal:       16777216 kB
        \\MemFree:         1048576 kB
        \\MemAvailable:    4194304 kB
        \\Buffers:          131072 kB
        \\Cached:          2097152 kB
        \\
    ) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(gib(16), info.total_bytes);
    try std.testing.expectEqual(@as(?usize, gib(4)), info.available_bytes);
}

test "linux meminfo parser has a conservative legacy availability fallback" {
    const info = parseLinuxMemInfo(
        \\MemTotal:        8388608 kB
        \\MemFree:          524288 kB
        \\Buffers:          131072 kB
        \\Cached:          1048576 kB
        \\SReclaimable:     262144 kB
        \\Shmem:            131072 kB
        \\
    ) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(gib(8), info.total_bytes);
    try std.testing.expectEqual(@as(?usize, 1792 * 1024 * 1024), info.available_bytes);
}

test "cgroup paths and limits constrain host memory" {
    const paths = parseCgroupPaths(
        \\0::/system.slice/antfly.service
        \\7:cpu,cpuacct:/system.slice/antfly.service
        \\6:memory:/production/antfly
        \\
    );
    try std.testing.expectEqualStrings("/system.slice/antfly.service", paths.v2.?);
    try std.testing.expectEqualStrings("/production/antfly", paths.v1_memory.?);

    const effective = applyCgroupMemoryInfo(
        .{ .total_bytes = gib(64), .available_bytes = gib(32) },
        .{ .limit_bytes = gib(16), .current_bytes = gib(12) },
    );
    try std.testing.expectEqual(gib(16), effective.total_bytes);
    try std.testing.expectEqual(@as(?usize, gib(4)), effective.available_bytes);

    const v1_unlimited = applyCgroupMemoryInfo(
        .{ .total_bytes = gib(64), .available_bytes = gib(32) },
        .{ .limit_bytes = std.math.maxInt(usize) - 4095, .current_bytes = gib(12) },
    );
    try std.testing.expectEqual(gib(64), v1_unlimited.total_bytes);
    try std.testing.expectEqual(@as(?usize, gib(32)), v1_unlimited.available_bytes);
}

test "cgroup mountinfo resolves controller roots and escaped mount paths" {
    var mounts = CgroupMounts{};
    parseCgroupMountInfoLine(
        "36 29 0:32 /kubepods.slice /run/antfly\\040cg rw,nosuid,nodev - cgroup2 cgroup rw",
        &mounts,
    );
    parseCgroupMountInfoLine(
        "44 29 0:40 /production /run/cgroup/memory rw - cgroup memory rw,memory",
        &mounts,
    );

    try std.testing.expect(mounts.v2.valid());
    try std.testing.expectEqualStrings("/kubepods.slice", mounts.v2.root());
    try std.testing.expectEqualStrings("/run/antfly cg", mounts.v2.mountPoint());
    try std.testing.expect(mounts.v1_memory.valid());
    try std.testing.expectEqualStrings("/production", mounts.v1_memory.root());
    try std.testing.expectEqualStrings("/run/cgroup/memory", mounts.v1_memory.mountPoint());
    try std.testing.expectEqualStrings(
        "/pod-a/container-b",
        cgroupPathRelativeToMount(
            "/kubepods.slice/pod-a/container-b",
            mounts.v2.root(),
        ).?,
    );
    try std.testing.expectEqualStrings(
        "/",
        cgroupPathRelativeToMount("/", mounts.v2.root()).?,
    );
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    try std.testing.expectEqualStrings(
        "/run/antfly cg/pod-a/container-b",
        cgroupDirectoryPath(
            &path_buffer,
            mounts.v2.mountPoint(),
            "/pod-a/container-b",
        ).?,
    );
}

test "canonical cgroup probe rejects ancestor-only subtree matches" {
    try std.testing.expect(!canonicalCgroupProbeIsAuthoritative(.{
        .info = .{ .limit_bytes = gib(8), .current_bytes = gib(6) },
        .leaf_present = false,
    }));
    try std.testing.expect(canonicalCgroupProbeIsAuthoritative(.{
        .info = .{ .limit_bytes = gib(2), .current_bytes = gib(1) },
        .leaf_present = true,
    }));
}

test "cgroup hierarchy uses finite parent limit beneath unlimited leaf" {
    var hierarchy = CgroupMemoryInfo{};
    // A v2 "max" leaf is represented as null and must not stop the ancestor walk.
    accumulateCgroupLevel(&hierarchy, null, gib(3));
    accumulateCgroupLevel(&hierarchy, gib(8), gib(6));
    accumulateCgroupLevel(&hierarchy, gib(16), gib(10));

    try std.testing.expectEqual(@as(?usize, gib(8)), hierarchy.limit_bytes);
    try std.testing.expectEqual(@as(?usize, gib(2)), hierarchy.available_bytes);
    const effective = applyCgroupMemoryInfo(
        .{ .total_bytes = gib(64), .available_bytes = gib(32) },
        hierarchy,
    );
    try std.testing.expectEqual(gib(8), effective.total_bytes);
    try std.testing.expectEqual(@as(?usize, gib(2)), effective.available_bytes);
}

test "live memory headroom scales down for constrained containers" {
    try std.testing.expectEqual(gib(1), liveHostMemoryHeadroom(gib(2)));
    try std.testing.expectEqual(gib(2), liveHostMemoryHeadroom(gib(4)));
    try std.testing.expectEqual(gib(3), liveHostMemoryHeadroom(gib(6)));
    try std.testing.expectEqual(gib(4), liveHostMemoryHeadroom(gib(8)));
    try std.testing.expectEqual(gib(6), liveHostMemoryHeadroom(gib(12)));
    try std.testing.expectEqual(gib(24), liveHostMemoryHeadroom(gib(128)));

    try checkLiveHostMemoryWithInfo(
        .{ .total_bytes = gib(2), .available_bytes = gib(2) },
        mib(128),
    );
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        checkLiveHostMemoryWithInfo(
            .{ .total_bytes = gib(2), .available_bytes = gib(1) },
            mib(128),
        ),
    );
}

test "derived host limits stay within constrained container safe pool" {
    const cpu = deriveLimitsForBackend(.cpu, .{
        .total_bytes = gib(2),
        .available_bytes = gib(2),
    });
    try std.testing.expectEqual(gib(1), cpu.host_limit_bytes);
    try std.testing.expectEqual(cpu.host_limit_bytes, cpu.combined_limit_bytes);
    try std.testing.expect(cpu.kv_limit_bytes <= cpu.host_limit_bytes);
    try std.testing.expect(cpu.scratch_limit_bytes <= cpu.host_limit_bytes);
    var controller = AdmissionController{};
    var first = try controller.tryAcquire(
        cpu,
        .{ .host_weight_bytes = mib(600) },
        false,
    );
    defer first.release();
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        controller.tryAcquire(
            cpu,
            .{ .host_weight_bytes = mib(600) },
            false,
        ),
    );

    const gpu = deriveLimitsForBackend(.gpu, .{
        .total_bytes = gib(2),
        .available_bytes = gib(2),
    });
    try std.testing.expect(gpu.host_limit_bytes <= gib(1));
}

test "shared admission accounts for concurrent leases and releases capacity" {
    var controller = AdmissionController{};
    const limits = Limits{
        .host_limit_bytes = 100,
        .combined_limit_bytes = 100,
        .kv_limit_bytes = 100,
        .scratch_limit_bytes = 100,
    };
    var first = try controller.tryAcquire(limits, .{ .host_weight_bytes = 60 }, false);
    defer first.release();
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        controller.tryAcquire(limits, .{ .host_kv_bytes = 50 }, false),
    );
    first.release();
    var second = try controller.tryAcquire(limits, .{ .host_kv_bytes = 50 }, false);
    defer second.release();
    try std.testing.expectEqual(@as(usize, 50), controller.snapshot().hostTotalBytes());
}

test "estimate reservations can be rolled back transactionally" {
    var budget = RunBudget.init(.{
        .host_limit_bytes = 100,
        .combined_limit_bytes = 100,
        .kv_limit_bytes = 100,
        .scratch_limit_bytes = 100,
    });
    const estimate = Estimate{
        .prompt_tokens = 1,
        .retained_tokens = 1,
        .kv_bytes = 60,
        .kv_tier = .host,
        .scratch_bytes = 30,
        .scratch_tier = .host,
    };
    try budget.reserveEstimate(estimate);
    try std.testing.expectEqual(@as(usize, 90), budget.hostTotalBytes());
    budget.releaseEstimate(estimate);
    try std.testing.expectEqual(@as(usize, 0), budget.hostTotalBytes());
    try std.testing.expectEqual(@as(usize, 0), budget.kvTotalBytes());
    try std.testing.expectEqual(@as(usize, 0), budget.scratchTotalBytes());

    const next_estimate = Estimate{
        .prompt_tokens = 1,
        .retained_tokens = 1,
        .kv_bytes = 70,
        .kv_tier = .host,
        .scratch_bytes = 30,
        .scratch_tier = .host,
    };
    try budget.reserveEstimate(next_estimate);
    try std.testing.expectEqual(@as(usize, 100), budget.hostTotalBytes());
    budget.releaseEstimate(next_estimate);
    try std.testing.expectEqual(@as(usize, 0), budget.hostTotalBytes());
}

test "single admission larger than policy is a resource limit" {
    var controller = AdmissionController{};
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        controller.tryAcquire(.{ .host_limit_bytes = 100 }, .{ .host_weight_bytes = 101 }, false),
    );
}

test "run budget enforces kv and scratch separately from host total" {
    var budget = RunBudget.init(.{
        .host_limit_bytes = 100,
        .backend_limit_bytes = 80,
        .combined_limit_bytes = 140,
        .kv_limit_bytes = 40,
        .scratch_limit_bytes = 20,
    });

    try budget.reserveEstimate(.{
        .prompt_tokens = 4,
        .retained_tokens = 8,
        .kv_bytes = 30,
        .kv_tier = .host,
        .scratch_bytes = 10,
        .scratch_tier = .host,
    });
    try std.testing.expectEqual(@as(usize, 40), budget.hostTotalBytes());
    try std.testing.expectError(error.MemoryBudgetExceeded, budget.tryReserveWeight(.host, 70));
    try std.testing.expect(budget.hasLastDenial());
    try std.testing.expectEqual(DenialLimit.host_total, budget.last_denial.?.limit);
    const reservation = try budget.tryReserveWeight(.host, 20);
    try std.testing.expectEqual(@as(usize, 60), budget.hostTotalBytes());
    budget.release(reservation);
    try std.testing.expectEqual(@as(usize, 40), budget.hostTotalBytes());
}

test "run budget formats denial details" {
    var budget = RunBudget.init(.{
        .host_limit_bytes = 64,
        .backend_limit_bytes = 0,
        .combined_limit_bytes = 64,
        .kv_limit_bytes = 0,
        .scratch_limit_bytes = 0,
    });
    try std.testing.expectError(error.MemoryBudgetExceeded, budget.tryReserveWeight(.host, 80));

    var buf: [256]u8 = undefined;
    const msg = try budget.lastDenialString(&buf);
    try std.testing.expect(std.mem.indexOf(u8, msg, "host_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "weight/host") != null);
}

test "run budget enforces combined host and backend total" {
    var budget = RunBudget.init(.{
        .host_limit_bytes = 0,
        .backend_limit_bytes = 0,
        .combined_limit_bytes = 100,
        .kv_limit_bytes = 0,
        .scratch_limit_bytes = 0,
    });

    _ = try budget.tryReserveWeight(.host, 60);
    try std.testing.expectError(error.MemoryBudgetExceeded, budget.tryReserveWeight(.backend, 50));
    try std.testing.expectEqual(DenialLimit.combined_total, budget.last_denial.?.limit);
}

test "gpt generation estimate accounts for sliding window and page alignment" {
    const cfg = gpt_mod.Config{
        .hidden_size = 4096,
        .num_hidden_layers = 32,
        .num_attention_heads = 32,
        .num_key_value_heads = 8,
        .attention_head_dim = 128,
        .vocab_size = 32000,
        .sliding_window = 4096,
        .position_encoding = .rope,
    };

    const estimate = try estimateGptGeneration(.metal, .f16, cfg, 100, 10, 64);
    try std.testing.expectEqual(@as(usize, 110), estimate.retained_tokens);
    try std.testing.expectEqual(@as(usize, 112), estimate.kv_bytes / (32 * 8 * 128 * 2 * 2));
    try std.testing.expectEqual(ResidencyTier.backend, estimate.kv_tier);
    try std.testing.expect(estimate.scratch_bytes > 0);

    // int8: bytesForTokenRow(8, 128) = 1024 + 8*4 = 1056
    const est_int8 = try estimateGptGeneration(.metal, .int8, cfg, 100, 10, 64);
    try std.testing.expectEqual(@as(usize, 112 * 32 * 1056 * 2), est_int8.kv_bytes);

    // int4: bytesForTokenRow(8, 128) = ceil(1024/32)*18 = 32*18 = 576
    const est_int4 = try estimateGptGeneration(.metal, .int4, cfg, 100, 10, 64);
    try std.testing.expectEqual(@as(usize, 112 * 32 * 576 * 2), est_int4.kv_bytes);
}

test "gpt generation estimate rejects malformed and overflowing inputs" {
    var cfg = gpt_mod.Config{
        .hidden_size = 4096,
        .num_hidden_layers = 32,
        .num_attention_heads = 32,
        .num_key_value_heads = 8,
        .attention_head_dim = 128,
        .vocab_size = 32000,
    };
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        estimateGptGeneration(.native, .f16, cfg, std.math.maxInt(usize), 1, 256),
    );

    cfg.num_attention_heads = 0;
    try std.testing.expectError(
        error.InvalidModelConfig,
        estimateGptGeneration(.native, .f16, cfg, 1, 1, 256),
    );

    cfg.num_attention_heads = 32;
    cfg.family = .deepseek_v4;
    cfg.deepseek_v4_kv_lora_rank = std.math.maxInt(u32);
    cfg.deepseek_v4_qk_rope_head_dim = std.math.maxInt(u32);
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        estimateGptGeneration(.native, .f16, cfg, 1, 1, 256),
    );
}

test "admission rejects aggregate total overflow" {
    var controller = AdmissionController{};
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        controller.tryAcquire(
            .{},
            .{ .host_weight_bytes = std.math.maxInt(usize), .host_kv_bytes = 1 },
            false,
        ),
    );
}

test "combined target and draft estimates are admitted atomically" {
    const target = AdmissionAmounts.fromEstimate(.{
        .prompt_tokens = 8,
        .retained_tokens = 16,
        .kv_bytes = 40,
        .kv_tier = .host,
        .scratch_bytes = 10,
        .scratch_tier = .host,
    });
    const draft = AdmissionAmounts.fromEstimate(.{
        .prompt_tokens = 8,
        .retained_tokens = 16,
        .kv_bytes = 30,
        .kv_tier = .backend,
        .scratch_bytes = 20,
        .scratch_tier = .backend,
    });
    const combined = try target.merge(draft);
    try std.testing.expectEqual(@as(usize, 50), combined.hostTotalBytes());
    try std.testing.expectEqual(@as(usize, 50), combined.backendTotalBytes());

    var controller = AdmissionController{};
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        controller.tryAcquire(.{ .combined_limit_bytes = 90 }, combined, false),
    );
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().hostTotalBytes());
}

test "run budget rejects accounting overflow even without configured limits" {
    var budget = RunBudget.init(.{});
    _ = try budget.tryReserveWeight(.host, std.math.maxInt(usize));
    try std.testing.expectError(error.MemoryBudgetExceeded, budget.tryReserveWeight(.host, 1));
    try std.testing.expectEqual(DenialLimit.host_total, budget.last_denial.?.limit);
    try std.testing.expectEqual(std.math.maxInt(usize), budget.hostTotalBytes());
}

test "derive gpu limits keeps combined cap sane" {
    const limits = deriveLimitsForBackend(.gpu, .{
        .total_bytes = gib(64),
        .available_bytes = gib(40),
    });
    try std.testing.expect(limits.combined_limit_bytes >= gib(6));
    try std.testing.expect(limits.combined_limit_bytes <= gib(12));
    try std.testing.expect(limits.backend_limit_bytes <= limits.combined_limit_bytes);
}
