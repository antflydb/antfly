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
const storage_runtime = @import("../kv/storage_runtime.zig");
const gpt_mod = @import("../../models/gpt.zig");

pub const generation_kv_page_size_tokens: u16 = 16;
pub const generation_default_prefill_chunk_tokens: usize = 256;
pub const generation_max_speculative_k: u32 = 16;
pub const generation_max_speculative_rows: usize = @as(usize, generation_max_speculative_k) + 1;

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
    pub extern fn memorystatus_get_level(level: *u32) c_int;
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

pub const bytes_per_mib: usize = 1024 * 1024;

/// CLI-facing memory limit overrides expressed in whole mebibytes. Zero means
/// that the corresponding limit is not overridden, preserving the established
/// command-line behavior while keeping the unit conversion checked and
/// centralized.
pub const BudgetOverridesMib = struct {
    host: usize = 0,
    backend: usize = 0,
    combined: usize = 0,
    kv: usize = 0,
    scratch: usize = 0,

    /// Convert the selectively populated override fields to their byte form.
    /// The returned `Limits` is an override set: zero fields remain omitted.
    pub fn toByteLimits(self: @This()) !Limits {
        return .{
            .host_limit_bytes = try mibToBytes(self.host),
            .backend_limit_bytes = try mibToBytes(self.backend),
            .combined_limit_bytes = try mibToBytes(self.combined),
            .kv_limit_bytes = try mibToBytes(self.kv),
            .scratch_limit_bytes = try mibToBytes(self.scratch),
        };
    }

    /// Apply explicit overrides after backend defaults and model-specific
    /// widening have been resolved.
    pub fn apply(self: @This(), defaults: Limits) !Limits {
        const overrides = try self.toByteLimits();
        var limits = defaults;
        if (overrides.host_limit_bytes != 0) limits.host_limit_bytes = overrides.host_limit_bytes;
        if (overrides.backend_limit_bytes != 0) limits.backend_limit_bytes = overrides.backend_limit_bytes;
        if (overrides.combined_limit_bytes != 0) limits.combined_limit_bytes = overrides.combined_limit_bytes;
        if (overrides.kv_limit_bytes != 0) limits.kv_limit_bytes = overrides.kv_limit_bytes;
        if (overrides.scratch_limit_bytes != 0) limits.scratch_limit_bytes = overrides.scratch_limit_bytes;
        return limits;
    }
};

fn mibToBytes(value: usize) !usize {
    return std.math.mul(usize, value, bytes_per_mib);
}

test "budget MiB overrides convert every memory domain exactly" {
    const overrides = try (BudgetOverridesMib{
        .host = 2,
        .backend = 3,
        .combined = 4,
        .kv = 5,
        .scratch = 6,
    }).toByteLimits();
    try std.testing.expectEqual(@as(usize, 2 * bytes_per_mib), overrides.host_limit_bytes);
    try std.testing.expectEqual(@as(usize, 3 * bytes_per_mib), overrides.backend_limit_bytes);
    try std.testing.expectEqual(@as(usize, 4 * bytes_per_mib), overrides.combined_limit_bytes);
    try std.testing.expectEqual(@as(usize, 5 * bytes_per_mib), overrides.kv_limit_bytes);
    try std.testing.expectEqual(@as(usize, 6 * bytes_per_mib), overrides.scratch_limit_bytes);
}

test "budget MiB overrides apply selectively and zero remains omitted" {
    const defaults = Limits{
        .host_limit_bytes = 10,
        .backend_limit_bytes = 20,
        .combined_limit_bytes = 30,
        .kv_limit_bytes = 40,
        .scratch_limit_bytes = 50,
    };
    const applied = try (BudgetOverridesMib{
        .host = 2,
        .kv = 3,
    }).apply(defaults);
    try std.testing.expectEqual(@as(usize, 2 * bytes_per_mib), applied.host_limit_bytes);
    try std.testing.expectEqual(@as(usize, 20), applied.backend_limit_bytes);
    try std.testing.expectEqual(@as(usize, 30), applied.combined_limit_bytes);
    try std.testing.expectEqual(@as(usize, 3 * bytes_per_mib), applied.kv_limit_bytes);
    try std.testing.expectEqual(@as(usize, 50), applied.scratch_limit_bytes);
    try std.testing.expectEqual(defaults, try (BudgetOverridesMib{}).apply(defaults));
}

test "budget MiB overrides accept the representable boundary and reject overflow" {
    const max_mib = std.math.maxInt(usize) / bytes_per_mib;
    const boundary = try (BudgetOverridesMib{ .host = max_mib }).toByteLimits();
    try std.testing.expectEqual(max_mib * bytes_per_mib, boundary.host_limit_bytes);

    const overflowing = max_mib + 1;
    try std.testing.expectError(error.Overflow, (BudgetOverridesMib{ .host = overflowing }).toByteLimits());
    try std.testing.expectError(error.Overflow, (BudgetOverridesMib{ .backend = overflowing }).toByteLimits());
    try std.testing.expectError(error.Overflow, (BudgetOverridesMib{ .combined = overflowing }).toByteLimits());
    try std.testing.expectError(error.Overflow, (BudgetOverridesMib{ .kv = overflowing }).toByteLimits());
    try std.testing.expectError(error.Overflow, (BudgetOverridesMib{ .scratch = overflowing }).toByteLimits());
}

pub fn maxCompositeLimits(a: Limits, b: Limits) Limits {
    return .{
        .host_limit_bytes = @max(a.host_limit_bytes, b.host_limit_bytes),
        .backend_limit_bytes = @max(a.backend_limit_bytes, b.backend_limit_bytes),
        .combined_limit_bytes = @max(a.combined_limit_bytes, b.combined_limit_bytes),
        .kv_limit_bytes = @max(a.kv_limit_bytes, b.kv_limit_bytes),
        .scratch_limit_bytes = @max(a.scratch_limit_bytes, b.scratch_limit_bytes),
    };
}

/// Stable node-wide limits for physical memory domains shared by otherwise
/// independent CPU/GPU workload policies. On discrete-GPU systems only host
/// RAM is shared; Metal allocations also consume the unified system-memory
/// domain.
pub const SharedAdmissionLimits = struct {
    host_limit_bytes: usize = 0,
    unified_limit_bytes: usize = 0,
};

pub const SystemMemoryInfo = struct {
    total_bytes: usize,
    available_bytes: ?usize = null,
    /// Describes how conservative `available_bytes` is. Linux MemAvailable
    /// already incorporates reclaimable caches and kernel reserves. The Mach
    /// page counters available to a daemon omit compressed/reclaimable memory,
    /// so applying the full node headroom to that snapshot double-reserves a
    /// large portion of RAM and can reject every positive admission while
    /// macOS still reports normal memory pressure.
    availability_basis: AvailabilityBasis = .mem_available,
};

pub const AvailabilityBasis = enum {
    mem_available,
    macos_memory_pressure,
    macos_reclaimable_pages,
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

/// Optional process-owner bridge for coordinating inference admission with a
/// broader resource manager. The inference runtime remains independent of the
/// owner implementation; leases mirror every acquire, retain, and release.
pub const AdmissionResourceError = error{
    ResourceLimitExceeded,
    ResourceTemporarilyUnavailable,
};

pub const AdmissionResourceBudget = struct {
    context: *anyopaque,
    /// Optional capability-context lifetime. Shared owners must provide both
    /// hooks so an admission controller can safely outlive its publisher.
    retain_context: ?*const fn (*anyopaque) bool = null,
    release_context: ?*const fn (*anyopaque) void = null,
    try_reserve: *const fn (*anyopaque, AdmissionAmounts) AdmissionResourceError!usize,
    retain: *const fn (*anyopaque, usize, AdmissionAmounts) AdmissionResourceError!void,
    release: *const fn (*anyopaque, usize) void,

    pub fn hasValidLifetimeHooks(self: @This()) bool {
        return (self.retain_context == null) == (self.release_context == null);
    }

    pub fn retainContext(self: @This()) bool {
        if (!self.hasValidLifetimeHooks()) return false;
        return if (self.retain_context) |retain| retain(self.context) else true;
    }

    pub fn releaseContext(self: @This()) void {
        if (self.release_context) |release| release(self.context);
    }
};

pub const AdmissionRequest = struct {
    backend_class: BackendClass,
    limits: Limits,
    amounts: AdmissionAmounts,
};

/// The exact transient constraint that rejected an admission. Callers that can
/// reclaim resident resources use this to choose a victim that can actually
/// relieve the failed budget instead of evicting by coarse byte-category
/// overlap. Permanent `ResourceLimitExceeded` failures never report pressure.
pub const AdmissionPressure = union(enum) {
    shared_host,
    shared_unified,
    domain_host: BackendClass,
    domain_backend: BackendClass,
    domain_combined: BackendClass,
    domain_kv: BackendClass,
    domain_scratch: BackendClass,
    external_budget,
    live_host,
};

const backend_class_count = 2;

fn backendClassIndex(backend_class: BackendClass) usize {
    return @intFromEnum(backend_class);
}

fn emptyAdmissionAmountsByBackend() [backend_class_count]AdmissionAmounts {
    return .{ .{}, .{} };
}

pub const AdmissionLease = struct {
    controller: ?*AdmissionController,
    amounts: AdmissionAmounts,
    amounts_by_backend: [backend_class_count]AdmissionAmounts,
    retain_backend_class: ?BackendClass,
    /// Opaque exactly-once ownership token returned by the external process
    /// manager. Byte totals are never reconstructed for release.
    external_lease: ?usize = null,
    /// Bytes committed against the current live-memory pressure epoch. This is
    /// deliberately separate from policy accounting: the kernel/cgroup probe
    /// reflects physical pressure, while AdmissionAmounts tracks ownership.
    live_reserved_bytes: usize,

    /// Reduce a peak reservation to the bytes that remain resident after a
    /// construction/import phase. This never acquires new capacity, so the
    /// transition cannot fail because of concurrent admissions.
    pub fn retain(self: *AdmissionLease, retained: AdmissionAmounts) !void {
        const controller = self.controller orelse return error.AdmissionLeaseReleased;
        const backend_class = self.retain_backend_class orelse
            return error.InvalidAdmissionLeaseReduction;
        const released = subtractAdmissionAmounts(self.amounts, retained) orelse
            return error.InvalidAdmissionLeaseReduction;
        const retained_live_bytes = liveHostBytes(retained) catch
            return error.InvalidAdmissionLeaseReduction;
        try controller.retainExternalLease(self.external_lease, retained);
        controller.releaseSingle(backend_class, released);
        controller.settleLiveReservation(
            self.live_reserved_bytes,
            retained_live_bytes,
        );
        self.amounts = retained;
        self.amounts_by_backend[backendClassIndex(backend_class)] = retained;
        self.live_reserved_bytes = 0;
    }

    pub fn release(self: *AdmissionLease) void {
        const controller = self.controller orelse return;
        controller.releaseLiveReservation(self.live_reserved_bytes);
        controller.release(self.amounts_by_backend, self.amounts, self.external_lease);
        self.controller = null;
        self.amounts = .{};
        self.amounts_by_backend = emptyAdmissionAmountsByBackend();
        self.retain_backend_class = null;
        self.external_lease = null;
        self.live_reserved_bytes = 0;
    }
};

pub const AdmissionController = struct {
    mutex: std.atomic.Mutex = .unlocked,
    /// Serializes reservations against one sampled live-memory capacity. While
    /// any lease is pending, every concurrent admission consumes the same
    /// sample instead of independently spending MemAvailable.
    live_mutex: std.atomic.Mutex = .unlocked,
    live_capacity_bytes: usize = 0,
    live_pending_bytes: usize = 0,
    /// Suppress a warning storm while repeated requests are denied by the
    /// same live-memory pressure condition. A successful new epoch clears it.
    live_denial_reported: bool = false,
    /// Aggregate accounting is retained for observability and for the optional
    /// process-owner resource budget. Host memory is enforced against the
    /// shared physical domain, while workload/device policy is enforced against
    /// the matching backend bucket below.
    admitted: AdmissionAmounts = .{},
    admitted_by_backend: [backend_class_count]AdmissionAmounts =
        emptyAdmissionAmountsByBackend(),
    shared_limits: SharedAdmissionLimits = .{},
    /// Optional hard envelope for the whole process/container. Unlike the
    /// inference-owned ledgers, the live check charges current container usage,
    /// including sibling processes and allocations outside inference.
    process_memory_limit_bytes: usize = 0,
    resource_budget: ?AdmissionResourceBudget = null,
    /// Opt-in fault injection for live server regression tests. Production
    /// callers leave this at zero, so normal admission behavior is unchanged.
    forced_run_denials_for_testing: std.atomic.Value(usize) = .init(0),

    pub fn deinit(self: *AdmissionController) void {
        self.configureResourceBudget(null) catch
            @panic("admission controller deinitialized with active resource leases");
    }

    pub fn configureForcedRunDenialsForTesting(
        self: *AdmissionController,
        count: usize,
    ) void {
        self.forced_run_denials_for_testing.store(count, .release);
    }

    pub fn consumeForcedRunDenialForTesting(
        self: *AdmissionController,
    ) bool {
        var remaining = self.forced_run_denials_for_testing.load(.acquire);
        while (remaining > 0) {
            remaining = self.forced_run_denials_for_testing.cmpxchgWeak(
                remaining,
                remaining - 1,
                .acq_rel,
                .acquire,
            ) orelse return true;
        }
        return false;
    }

    pub fn configureSharedLimits(
        self: *AdmissionController,
        shared_limits: SharedAdmissionLimits,
    ) void {
        spinLockAdmission(&self.mutex);
        defer self.mutex.unlock();
        std.debug.assert(std.meta.eql(self.admitted, AdmissionAmounts{}));
        self.shared_limits = shared_limits;
    }

    pub fn configureProcessMemoryLimit(
        self: *AdmissionController,
        limit_bytes: usize,
    ) void {
        spinLockAdmission(&self.mutex);
        defer self.mutex.unlock();
        std.debug.assert(std.meta.eql(self.admitted, AdmissionAmounts{}));
        self.process_memory_limit_bytes = limit_bytes;
    }

    pub fn configureResourceBudget(
        self: *AdmissionController,
        resource_budget: ?AdmissionResourceBudget,
    ) !void {
        if (resource_budget) |budget| {
            if (!budget.hasValidLifetimeHooks())
                return error.InvalidResourceBudgetLifetime;
            if (!budget.retainContext())
                return error.ResourceOwnerShuttingDown;
        }
        errdefer if (resource_budget) |budget| budget.releaseContext();
        spinLockAdmission(&self.mutex);
        if (!std.meta.eql(self.admitted, AdmissionAmounts{})) {
            self.mutex.unlock();
            return error.ResourceBudgetInUse;
        }
        const previous = self.resource_budget;
        self.resource_budget = resource_budget;
        self.mutex.unlock();
        if (previous) |budget| budget.releaseContext();
    }

    pub fn tryAcquire(
        self: *AdmissionController,
        backend_class: BackendClass,
        limits: Limits,
        amounts: AdmissionAmounts,
        check_live_memory: bool,
    ) !AdmissionLease {
        var ignored_pressure: ?AdmissionPressure = null;
        return self.tryAcquireWithPressure(
            backend_class,
            limits,
            amounts,
            check_live_memory,
            &ignored_pressure,
        );
    }

    pub fn tryAcquireWithPressure(
        self: *AdmissionController,
        backend_class: BackendClass,
        limits: Limits,
        amounts: AdmissionAmounts,
        check_live_memory: bool,
        pressure: *?AdmissionPressure,
    ) !AdmissionLease {
        return self.tryAcquireRequestsWithPressure(
            &.{.{
                .backend_class = backend_class,
                .limits = limits,
                .amounts = amounts,
            }},
            check_live_memory,
            pressure,
        );
    }

    /// Atomically acquire one or more CPU/GPU resource domains. Requests in the
    /// same domain are merged under the strictest non-zero limit, while requests
    /// in different domains are checked independently. Aggregate bytes continue
    /// to be reserved once against the process-owner resource budget.
    pub fn tryAcquireRequests(
        self: *AdmissionController,
        requests: []const AdmissionRequest,
        check_live_memory: bool,
    ) !AdmissionLease {
        var ignored_pressure: ?AdmissionPressure = null;
        return self.tryAcquireRequestsWithPressure(
            requests,
            check_live_memory,
            &ignored_pressure,
        );
    }

    pub fn tryAcquireRequestsWithPressure(
        self: *AdmissionController,
        requests: []const AdmissionRequest,
        check_live_memory: bool,
        pressure: *?AdmissionPressure,
    ) !AdmissionLease {
        pressure.* = null;
        var amounts_by_backend = emptyAdmissionAmountsByBackend();
        var limits_by_backend = [_]Limits{ .{}, .{} };
        var active_backends = [_]bool{ false, false };
        var amounts: AdmissionAmounts = .{};

        for (requests) |request| {
            const index = backendClassIndex(request.backend_class);
            amounts_by_backend[index] = amounts_by_backend[index].merge(request.amounts) catch
                return error.ResourceLimitExceeded;
            limits_by_backend[index] = if (active_backends[index])
                intersectAdmissionLimits(limits_by_backend[index], request.limits)
            else
                request.limits;
            active_backends[index] = true;
            amounts = amounts.merge(request.amounts) catch return error.ResourceLimitExceeded;
        }

        const request_host = amounts.hostTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const request_backend = amounts.backendTotalBytesChecked() catch return error.ResourceLimitExceeded;
        const request_combined = std.math.add(usize, request_host, request_backend) catch
            return error.ResourceLimitExceeded;

        // Publish a provisional reservation while holding only the in-memory
        // accounting lock. The potentially blocking OS/cgroup probe runs after
        // unlock; concurrent requests see the provisional bytes and therefore
        // cannot all pass stable limits before memory.current catches up.
        {
            spinLockAdmission(&self.mutex);
            defer self.mutex.unlock();

            const next = addAdmissionAmounts(self.admitted, amounts) orelse
                return error.ResourceLimitExceeded;
            const next_global_host = next.hostTotalBytesChecked() catch
                return error.ResourceLimitExceeded;
            const next_global_backend = next.backendTotalBytesChecked() catch
                return error.ResourceLimitExceeded;
            _ = std.math.add(usize, next_global_host, next_global_backend) catch
                return error.ResourceLimitExceeded;
            _ = next.kvTotalBytesChecked() catch return error.ResourceLimitExceeded;
            _ = next.scratchTotalBytesChecked() catch return error.ResourceLimitExceeded;

            // CPU work and CUDA staging/cache allocations consume the same host
            // RAM. Enforce that physical domain before backend-local policies so
            // provisional cross-backend reservations cannot race past it.
            try checkAdmissionLimitWithPressure(
                request_host,
                next_global_host,
                self.shared_limits.host_limit_bytes,
                .shared_host,
                pressure,
            );
            if (builtin.os.tag == .macos) {
                try checkAdmissionLimitWithPressure(
                    request_combined,
                    std.math.add(
                        usize,
                        next_global_host,
                        next_global_backend,
                    ) catch return error.ResourceLimitExceeded,
                    self.shared_limits.unified_limit_bytes,
                    .shared_unified,
                    pressure,
                );
            }

            var next_by_backend = self.admitted_by_backend;
            for (0..backend_class_count) |index| {
                if (!active_backends[index]) continue;
                const request = amounts_by_backend[index];
                const limits = limits_by_backend[index];
                const domain_next = addAdmissionAmounts(next_by_backend[index], request) orelse
                    return error.ResourceLimitExceeded;
                const domain_request_host = request.hostTotalBytesChecked() catch
                    return error.ResourceLimitExceeded;
                const domain_request_backend = request.backendTotalBytesChecked() catch
                    return error.ResourceLimitExceeded;
                const domain_request_combined = std.math.add(
                    usize,
                    domain_request_host,
                    domain_request_backend,
                ) catch return error.ResourceLimitExceeded;
                const domain_request_kv = request.kvTotalBytesChecked() catch
                    return error.ResourceLimitExceeded;
                const domain_request_scratch = request.scratchTotalBytesChecked() catch
                    return error.ResourceLimitExceeded;
                const next_host = domain_next.hostTotalBytesChecked() catch
                    return error.ResourceLimitExceeded;
                const next_backend = domain_next.backendTotalBytesChecked() catch
                    return error.ResourceLimitExceeded;
                const next_combined = std.math.add(usize, next_host, next_backend) catch
                    return error.ResourceLimitExceeded;
                const next_kv = domain_next.kvTotalBytesChecked() catch
                    return error.ResourceLimitExceeded;
                const next_scratch = domain_next.scratchTotalBytesChecked() catch
                    return error.ResourceLimitExceeded;

                const backend_class: BackendClass = @enumFromInt(index);
                try checkAdmissionLimitWithPressure(
                    domain_request_host,
                    next_host,
                    limits.host_limit_bytes,
                    .{ .domain_host = backend_class },
                    pressure,
                );
                try checkAdmissionLimitWithPressure(
                    domain_request_backend,
                    next_backend,
                    limits.backend_limit_bytes,
                    .{ .domain_backend = backend_class },
                    pressure,
                );
                try checkAdmissionLimitWithPressure(
                    domain_request_combined,
                    next_combined,
                    limits.combined_limit_bytes,
                    .{ .domain_combined = backend_class },
                    pressure,
                );
                try checkAdmissionLimitWithPressure(
                    domain_request_kv,
                    next_kv,
                    limits.kv_limit_bytes,
                    .{ .domain_kv = backend_class },
                    pressure,
                );
                try checkAdmissionLimitWithPressure(
                    domain_request_scratch,
                    next_scratch,
                    limits.scratch_limit_bytes,
                    .{ .domain_scratch = backend_class },
                    pressure,
                );
                next_by_backend[index] = domain_next;
            }
            self.admitted = next;
            self.admitted_by_backend = next_by_backend;
        }

        var external_lease: ?usize = null;
        if (self.resource_budget) |resource_budget| {
            const token = resource_budget.try_reserve(resource_budget.context, amounts) catch |err| {
                if (err == error.ResourceTemporarilyUnavailable)
                    pressure.* = .external_budget;
                self.releaseLocal(amounts_by_backend, amounts);
                return err;
            };
            if (token == 0) {
                self.releaseLocal(amounts_by_backend, amounts);
                return error.ResourceLimitExceeded;
            }
            external_lease = token;
        }

        var live_reserved_bytes: usize = 0;
        if (check_live_memory) {
            // Metal allocations consume unified system memory. CUDA allocations are
            // accounted against the backend budget and must not also be charged to
            // Linux MemAvailable, or a valid device-resident model is rejected merely
            // because its VRAM footprint exceeds free host RAM.
            const live_host_incremental = liveHostBytes(amounts) catch {
                self.release(amounts_by_backend, amounts, external_lease);
                return error.ResourceLimitExceeded;
            };
            live_reserved_bytes = self.tryReserveLiveCapacity(live_host_incremental) catch |err| {
                if (err == error.ResourceTemporarilyUnavailable)
                    pressure.* = .live_host;
                self.release(amounts_by_backend, amounts, external_lease);
                return err;
            };
        }
        var retain_backend_class: ?BackendClass = null;
        for (active_backends, 0..) |active, index| {
            if (!active) continue;
            if (retain_backend_class != null) {
                retain_backend_class = null;
                break;
            }
            retain_backend_class = @enumFromInt(index);
        }
        return .{
            .controller = self,
            .amounts = amounts,
            .amounts_by_backend = amounts_by_backend,
            .retain_backend_class = retain_backend_class,
            .external_lease = external_lease,
            .live_reserved_bytes = live_reserved_bytes,
        };
    }

    pub fn snapshot(self: *AdmissionController) AdmissionAmounts {
        spinLockAdmission(&self.mutex);
        defer self.mutex.unlock();
        return self.admitted;
    }

    pub fn snapshotBackend(
        self: *AdmissionController,
        backend_class: BackendClass,
    ) AdmissionAmounts {
        spinLockAdmission(&self.mutex);
        defer self.mutex.unlock();
        return self.admitted_by_backend[backendClassIndex(backend_class)];
    }

    fn releaseSingle(
        self: *AdmissionController,
        backend_class: BackendClass,
        amounts: AdmissionAmounts,
    ) void {
        var amounts_by_backend = emptyAdmissionAmountsByBackend();
        amounts_by_backend[backendClassIndex(backend_class)] = amounts;
        self.releaseLocal(amounts_by_backend, amounts);
    }

    fn release(
        self: *AdmissionController,
        amounts_by_backend: [backend_class_count]AdmissionAmounts,
        amounts: AdmissionAmounts,
        external_lease: ?usize,
    ) void {
        if (external_lease) |token| {
            // Keep the local reservation published until the external owner has
            // consumed its exactly-once token. configureResourceBudget() uses
            // that reservation as its exclusion condition, so releasing local
            // accounting first would let a concurrent reconfiguration detach
            // the old context and route this token to a different capability.
            const resource_budget = self.resource_budget orelse
                @panic("external admission lease lost its resource budget");
            resource_budget.release(resource_budget.context, token);
        }
        self.releaseLocal(amounts_by_backend, amounts);
    }

    fn retainExternalLease(
        self: *AdmissionController,
        external_lease: ?usize,
        retained: AdmissionAmounts,
    ) AdmissionResourceError!void {
        const token = external_lease orelse return;
        const resource_budget = self.resource_budget orelse
            return error.ResourceLimitExceeded;
        try resource_budget.retain(resource_budget.context, token, retained);
    }

    fn releaseLocal(
        self: *AdmissionController,
        amounts_by_backend: [backend_class_count]AdmissionAmounts,
        amounts: AdmissionAmounts,
    ) void {
        spinLockAdmission(&self.mutex);
        defer self.mutex.unlock();
        subtractAdmissionAmountsInPlace(&self.admitted, amounts);
        for (0..backend_class_count) |index|
            subtractAdmissionAmountsInPlace(
                &self.admitted_by_backend[index],
                amounts_by_backend[index],
            );
    }

    fn tryReserveLiveCapacity(
        self: *AdmissionController,
        incremental_bytes: usize,
    ) !usize {
        if (incremental_bytes == 0) return 0;
        spinLockAdmission(&self.live_mutex);
        defer self.live_mutex.unlock();

        const info = if (self.live_pending_bytes == 0)
            currentSystemMemoryInfoForLimit(self.process_memory_limit_bytes)
        else
            null;
        const starts_new_epoch = self.live_pending_bytes == 0;
        const reserved = self.tryReserveLiveCapacityLocked(incremental_bytes, info) catch |err| {
            if (err == error.ResourceTemporarilyUnavailable and !self.live_denial_reported) {
                if (info) |memory_info| {
                    const available = memory_info.available_bytes orelse 0;
                    const reserve = liveHostAdmissionReserve(memory_info);
                    std.log.warn(
                        "inference live-memory admission denied requested_bytes={d} pending_bytes={d} available_bytes={d} reserve_bytes={d} capacity_bytes={d} basis={s}",
                        .{
                            incremental_bytes,
                            self.live_pending_bytes,
                            available,
                            reserve,
                            available -| reserve,
                            @tagName(memory_info.availability_basis),
                        },
                    );
                } else {
                    std.log.warn(
                        "inference live-memory admission denied requested_bytes={d} pending_bytes={d} capacity_bytes={d} basis=shared_epoch",
                        .{ incremental_bytes, self.live_pending_bytes, self.live_capacity_bytes },
                    );
                }
                self.live_denial_reported = true;
            }
            return err;
        };
        if (starts_new_epoch) self.live_denial_reported = false;
        return reserved;
    }

    fn tryReserveLiveCapacityWithInfo(
        self: *AdmissionController,
        incremental_bytes: usize,
        info: SystemMemoryInfo,
    ) !usize {
        if (incremental_bytes == 0) return 0;
        spinLockAdmission(&self.live_mutex);
        defer self.live_mutex.unlock();
        return self.tryReserveLiveCapacityLocked(incremental_bytes, info);
    }

    fn tryReserveLiveCapacityLocked(
        self: *AdmissionController,
        incremental_bytes: usize,
        info: ?SystemMemoryInfo,
    ) !usize {
        std.debug.assert(incremental_bytes > 0);
        if (self.live_pending_bytes == 0) {
            const memory_info = info orelse return 0;
            const available = memory_info.available_bytes orelse return 0;
            const reserve = liveHostAdmissionReserve(memory_info);
            self.live_capacity_bytes = available -| reserve;
        }

        const next_pending = std.math.add(
            usize,
            self.live_pending_bytes,
            incremental_bytes,
        ) catch {
            if (self.live_pending_bytes == 0) self.live_capacity_bytes = 0;
            return error.ResourceTemporarilyUnavailable;
        };
        if (next_pending > self.live_capacity_bytes) {
            if (self.live_pending_bytes == 0) self.live_capacity_bytes = 0;
            return error.ResourceTemporarilyUnavailable;
        }
        self.live_pending_bytes = next_pending;
        return incremental_bytes;
    }

    /// Construction/import peaks are no longer pending once the session is
    /// live. Preserve the capacity consumed by resident physical memory until
    /// the pressure epoch drains; a subsequent epoch re-samples the kernel.
    fn settleLiveReservation(
        self: *AdmissionController,
        reserved_bytes: usize,
        retained_bytes: usize,
    ) void {
        if (reserved_bytes == 0) return;
        std.debug.assert(retained_bytes <= reserved_bytes);
        spinLockAdmission(&self.live_mutex);
        defer self.live_mutex.unlock();
        std.debug.assert(reserved_bytes <= self.live_pending_bytes);
        self.live_pending_bytes -= reserved_bytes;
        self.live_capacity_bytes -|= retained_bytes;
        if (self.live_pending_bytes == 0) self.live_capacity_bytes = 0;
    }

    fn releaseLiveReservation(
        self: *AdmissionController,
        reserved_bytes: usize,
    ) void {
        if (reserved_bytes == 0) return;
        spinLockAdmission(&self.live_mutex);
        defer self.live_mutex.unlock();
        std.debug.assert(reserved_bytes <= self.live_pending_bytes);
        self.live_pending_bytes -= reserved_bytes;
        if (self.live_pending_bytes == 0) self.live_capacity_bytes = 0;
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

fn intersectAdmissionLimit(a: usize, b: usize) usize {
    if (a == 0) return b;
    if (b == 0) return a;
    return @min(a, b);
}

fn intersectAdmissionLimits(a: Limits, b: Limits) Limits {
    return .{
        .host_limit_bytes = intersectAdmissionLimit(a.host_limit_bytes, b.host_limit_bytes),
        .backend_limit_bytes = intersectAdmissionLimit(a.backend_limit_bytes, b.backend_limit_bytes),
        .combined_limit_bytes = intersectAdmissionLimit(a.combined_limit_bytes, b.combined_limit_bytes),
        .kv_limit_bytes = intersectAdmissionLimit(a.kv_limit_bytes, b.kv_limit_bytes),
        .scratch_limit_bytes = intersectAdmissionLimit(a.scratch_limit_bytes, b.scratch_limit_bytes),
    };
}

fn subtractAdmissionAmountsInPlace(
    total: *AdmissionAmounts,
    amounts: AdmissionAmounts,
) void {
    total.host_weight_bytes -|= amounts.host_weight_bytes;
    total.backend_weight_bytes -|= amounts.backend_weight_bytes;
    total.host_kv_bytes -|= amounts.host_kv_bytes;
    total.backend_kv_bytes -|= amounts.backend_kv_bytes;
    total.host_scratch_bytes -|= amounts.host_scratch_bytes;
    total.backend_scratch_bytes -|= amounts.backend_scratch_bytes;
}

fn subtractAdmissionAmounts(
    total: AdmissionAmounts,
    retained: AdmissionAmounts,
) ?AdmissionAmounts {
    if (retained.host_weight_bytes > total.host_weight_bytes or
        retained.backend_weight_bytes > total.backend_weight_bytes or
        retained.host_kv_bytes > total.host_kv_bytes or
        retained.backend_kv_bytes > total.backend_kv_bytes or
        retained.host_scratch_bytes > total.host_scratch_bytes or
        retained.backend_scratch_bytes > total.backend_scratch_bytes)
    {
        return null;
    }
    return .{
        .host_weight_bytes = total.host_weight_bytes - retained.host_weight_bytes,
        .backend_weight_bytes = total.backend_weight_bytes - retained.backend_weight_bytes,
        .host_kv_bytes = total.host_kv_bytes - retained.host_kv_bytes,
        .backend_kv_bytes = total.backend_kv_bytes - retained.backend_kv_bytes,
        .host_scratch_bytes = total.host_scratch_bytes - retained.host_scratch_bytes,
        .backend_scratch_bytes = total.backend_scratch_bytes - retained.backend_scratch_bytes,
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

fn checkAdmissionLimitWithPressure(
    request: usize,
    next: usize,
    limit: usize,
    rejected_pressure: AdmissionPressure,
    pressure: *?AdmissionPressure,
) !void {
    checkAdmissionLimit(request, next, limit) catch |err| {
        if (err == error.ResourceTemporarilyUnavailable)
            pressure.* = rejected_pressure;
        return err;
    };
}

fn checkLiveHostMemory(incremental_bytes: usize) !void {
    const info = currentSystemMemoryInfo() orelse return;
    return checkLiveHostMemoryWithInfo(info, incremental_bytes);
}

fn liveHostBytes(amounts: AdmissionAmounts) !usize {
    const host = try amounts.hostTotalBytesChecked();
    if (builtin.os.tag != .macos) return host;
    return std.math.add(
        usize,
        host,
        try amounts.backendTotalBytesChecked(),
    );
}

/// Preserve the release default on large hosts while guaranteeing that a
/// container or smaller machine can use at least half of its effective memory.
/// A fixed multi-GiB floor cannot be applied after cgroup constraints: when the
/// effective total is below that floor, every positive admission is impossible.
fn liveHostMemoryHeadroom(total_bytes: usize) usize {
    const preferred = clampBytes(@max(total_bytes / 4, gib(6)), gib(4), gib(24));
    return @min(preferred, total_bytes / 2);
}

/// macOS pressure availability and the raw-page fallback both need a bounded
/// small-load grace window below the stable node headroom. Keeping the window
/// proportional to the machine while capped prevents a large model from
/// consuming most of the remaining reclaimable capacity merely because the
/// system-wide residency limit has room.
fn macosLiveAdmissionGrace(total_bytes: usize) usize {
    return clampBytes(total_bytes / 32, mib(256), gib(2));
}

/// Determine how much of the current availability sample must remain unused.
///
/// Linux MemAvailable is an allocation-availability estimate. The stable shared
/// admission limit already retains full node headroom against Antfly residency,
/// so this second, live-pressure gate preserves half of current availability,
/// with a 512 MiB emergency floor and the stable node headroom as a ceiling.
/// This keeps a shared runner usable without reducing request capacity to zero
/// merely because an already-admitted model lowered MemAvailable below half of
/// the stable headroom. macOS supplies a system pressure percentage, with raw
/// Mach page queues as a conservative fallback, and uses a smaller bounded
/// grace window because its availability sample omits some reclaimable memory.
fn liveHostAdmissionReserve(info: SystemMemoryInfo) usize {
    const available = info.available_bytes orelse return 0;
    const node_headroom = liveHostMemoryHeadroom(info.total_bytes);
    return switch (info.availability_basis) {
        .mem_available => @min(
            node_headroom,
            @min(available, @max(mib(512), available / 2)),
        ),
        .macos_memory_pressure, .macos_reclaimable_pages => blk: {
            const normal_capacity = available -| node_headroom;
            const emergency_reserve = @min(
                available,
                @max(mib(512), available / 4),
            );
            const grace_capacity = @min(
                macosLiveAdmissionGrace(info.total_bytes),
                available -| emergency_reserve,
            );
            const capacity = @max(normal_capacity, grace_capacity);
            break :blk available -| capacity;
        },
    };
}

fn checkLiveHostMemoryWithInfo(info: SystemMemoryInfo, incremental_bytes: usize) !void {
    const available = info.available_bytes orelse return;
    const reserve = liveHostAdmissionReserve(info);
    const required = std.math.add(usize, incremental_bytes, reserve) catch
        return error.ResourceLimitExceeded;
    if (available < required) return error.ResourceTemporarilyUnavailable;
}

pub fn defaultLimitsForBackend(backend: BackendClass) Limits {
    return defaultLimitsForBackendWithProcessLimit(backend, 0);
}

pub fn defaultLimitsForBackendWithProcessLimit(
    backend: BackendClass,
    process_memory_limit_bytes: usize,
) Limits {
    if (stableSystemMemoryInfoForLimit(process_memory_limit_bytes)) |info| {
        // Admission policy is stable for the lifetime of a machine configuration.
        // Current pressure is checked separately immediately before allocation.
        return deriveLimitsForBackend(backend, .{
            .total_bytes = info.total_bytes,
            .available_bytes = info.total_bytes,
        });
    }
    return staticLimitsForBackend(backend);
}

/// Derive one stable physical-memory policy for the process. Backend-local
/// defaults intentionally remain more conservative workload caps; this shared
/// cap represents all memory available after the node's safety headroom and
/// therefore also accommodates explicitly widened large-model floors.
pub fn defaultSharedAdmissionLimits() SharedAdmissionLimits {
    return defaultSharedAdmissionLimitsWithProcessLimit(0);
}

pub fn defaultSharedAdmissionLimitsWithProcessLimit(
    process_memory_limit_bytes: usize,
) SharedAdmissionLimits {
    const defaults = if (stableSystemMemoryInfoForLimit(process_memory_limit_bytes)) |info| blk: {
        const safe_pool = info.total_bytes -|
            @min(info.total_bytes, liveHostMemoryHeadroom(info.total_bytes));
        break :blk SharedAdmissionLimits{
            .host_limit_bytes = safe_pool,
            .unified_limit_bytes = if (builtin.os.tag == .macos) safe_pool else 0,
        };
    } else blk: {
        const cpu = staticLimitsForBackend(.cpu);
        const gpu = staticLimitsForBackend(.gpu);
        break :blk SharedAdmissionLimits{
            .host_limit_bytes = @max(cpu.host_limit_bytes, gpu.host_limit_bytes),
            .unified_limit_bytes = if (builtin.os.tag == .macos)
                @max(cpu.combined_limit_bytes, gpu.combined_limit_bytes)
            else
                0,
        };
    };
    return defaults;
}

/// Node overrides are global policy for shared physical domains, while the
/// remaining fields continue to constrain each CPU/GPU workload bucket.
pub fn sharedAdmissionLimitsWithOverrides(overrides: Limits) SharedAdmissionLimits {
    return sharedAdmissionLimitsWithOverridesAndProcessLimit(overrides, 0);
}

pub fn sharedAdmissionLimitsWithOverridesAndProcessLimit(
    overrides: Limits,
    process_memory_limit_bytes: usize,
) SharedAdmissionLimits {
    var limits = defaultSharedAdmissionLimitsWithProcessLimit(process_memory_limit_bytes);
    if (overrides.host_limit_bytes > 0)
        limits.host_limit_bytes = overrides.host_limit_bytes;
    if (builtin.os.tag == .macos and overrides.combined_limit_bytes > 0)
        limits.unified_limit_bytes = overrides.combined_limit_bytes;
    return limits;
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
        .linux => probeSystemMemoryInfoLinuxSample().info,
        else => null,
    };
}

/// Apply an operator-owned process/container envelope to the live memory view.
/// On Linux the explicit limit is charged against raw leaf cgroup usage, so
/// page cache, a test harness, and sibling processes remain visible even when
/// memory.max itself is unlimited.
pub fn currentSystemMemoryInfoForLimit(limit_bytes: usize) ?SystemMemoryInfo {
    return switch (builtin.os.tag) {
        .linux => blk: {
            const sample = probeSystemMemoryInfoLinuxSample();
            break :blk applyOptionalProcessMemoryLimit(
                sample.info,
                limit_bytes,
                sample.cgroup.leaf_current_bytes,
            );
        },
        .macos => applyOptionalProcessMemoryLimit(
            probeSystemMemoryInfoMacos(),
            limit_bytes,
            null,
        ),
        else => applyOptionalProcessMemoryLimit(null, limit_bytes, null),
    };
}

fn applyOptionalProcessMemoryLimit(
    detected: ?SystemMemoryInfo,
    limit_bytes: usize,
    leaf_current_bytes: ?usize,
) ?SystemMemoryInfo {
    if (limit_bytes == 0) return detected;
    return applyProcessMemoryLimit(detected orelse .{
        .total_bytes = limit_bytes,
        .available_bytes = limit_bytes,
        .availability_basis = .mem_available,
    }, limit_bytes, leaf_current_bytes);
}

fn applyProcessMemoryLimit(
    detected: SystemMemoryInfo,
    limit_bytes: usize,
    leaf_current_bytes: ?usize,
) SystemMemoryInfo {
    const effective_limit = @min(detected.total_bytes, limit_bytes);
    var available = @min(detected.available_bytes orelse effective_limit, effective_limit);
    if (leaf_current_bytes) |current| {
        available = @min(available, effective_limit -| @min(current, effective_limit));
    }
    return .{
        .total_bytes = effective_limit,
        .available_bytes = available,
        .availability_basis = detected.availability_basis,
    };
}

fn stableSystemMemoryInfoForLimit(limit_bytes: usize) ?SystemMemoryInfo {
    return stableMemoryInfoForLimit(currentSystemMemoryInfo(), limit_bytes);
}

fn stableMemoryInfoForLimit(detected: ?SystemMemoryInfo, limit_bytes: usize) ?SystemMemoryInfo {
    if (limit_bytes == 0) return detected;
    const baseline = detected orelse SystemMemoryInfo{
        .total_bytes = limit_bytes,
        .available_bytes = limit_bytes,
        .availability_basis = .mem_available,
    };
    const effective_limit = @min(baseline.total_bytes, limit_bytes);
    return .{
        .total_bytes = effective_limit,
        .available_bytes = effective_limit,
        .availability_basis = baseline.availability_basis,
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

    const reclaimable_pages = macosReclaimablePageCount(
        vm_stats.free_count,
        vm_stats.inactive_count,
        vm_stats.speculative_count,
    );
    const reclaimable_bytes = reclaimable_pages * @as(u64, @intCast(page_size));
    var pressure_level: u32 = 0;
    const pressure_available_bytes = if (macos.memorystatus_get_level(&pressure_level) == 0)
        macosMemoryPressureAvailableBytes(total_raw, pressure_level)
    else
        null;
    return .{
        .total_bytes = @intCast(total_raw),
        // `memorystatus_get_level` is the same system-wide free percentage
        // reported by memory_pressure(1). Unlike the raw Mach page queues, it
        // accounts for reclaimable file cache and therefore remains meaningful
        // on warm macOS hosts. Keep the page-queue estimate as a fallback.
        .available_bytes = @intCast(@min(pressure_available_bytes orelse reclaimable_bytes, total_raw)),
        .availability_basis = if (pressure_available_bytes != null)
            .macos_memory_pressure
        else
            .macos_reclaimable_pages,
    };
}

fn macosMemoryPressureAvailableBytes(total_bytes: u64, free_percent: u32) ?u64 {
    if (free_percent > 100) return null;
    return @intCast((@as(u128, total_bytes) * free_percent) / 100);
}

/// Mach reports speculative pages as a subset of `free_count`; adding
/// `speculative_count` again can overstate availability by multiple GiB.
fn macosReclaimablePageCount(
    free_count: u32,
    inactive_count: u32,
    speculative_count: u32,
) u64 {
    _ = speculative_count;
    return @as(u64, free_count) + @as(u64, inactive_count);
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
    /// Current usage at the process leaf, retained even when memory.max is
    /// unlimited so an explicit process envelope can still be enforced.
    leaf_current_bytes: ?usize = null,
};

const LinuxSystemMemorySample = struct {
    info: ?SystemMemoryInfo = null,
    cgroup: CgroupMemoryInfo = .{},
};

const CgroupVersion = enum {
    v1,
    v2,
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

fn isSafeAbsoluteCgroupPath(path: []const u8) bool {
    if (path.len == 0 or path[0] != '/' or
        std.mem.indexOfScalar(u8, path, 0) != null)
    {
        return false;
    }
    var components = std.mem.splitScalar(u8, path, '/');
    while (components.next()) |component| {
        if (std.mem.eql(u8, component, ".") or
            std.mem.eql(u8, component, ".."))
        {
            return false;
        }
    }
    return true;
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
        if (!isSafeAbsoluteCgroupPath(path)) continue;
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

fn parseCgroupInactiveFileBytes(
    bytes: []const u8,
    version: CgroupVersion,
) ?usize {
    var inactive_file: ?usize = null;
    var total_inactive_file: ?usize = null;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |line| {
        var fields = std.mem.tokenizeAny(u8, line, " \t");
        const key = fields.next() orelse continue;
        const raw = fields.next() orelse continue;
        const value = std.fmt.parseUnsigned(usize, raw, 10) catch continue;
        if (std.mem.eql(u8, key, "inactive_file")) {
            inactive_file = value;
        } else if (std.mem.eql(u8, key, "total_inactive_file")) {
            total_inactive_file = value;
        }
    }
    return switch (version) {
        .v1 => total_inactive_file orelse inactive_file,
        .v2 => inactive_file,
    };
}

fn readCgroupInactiveFileBytes(
    path: []const u8,
    version: CgroupVersion,
) ?usize {
    var buffer: [8192]u8 = undefined;
    const bytes = readSmallLinuxFile(path, &buffer) orelse return null;
    return parseCgroupInactiveFileBytes(bytes, version);
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

fn mergeCgroupMemoryInfo(
    result: *CgroupMemoryInfo,
    candidate: CgroupMemoryInfo,
) void {
    if (candidate.leaf_current_bytes) |current|
        result.leaf_current_bytes = current;
    if (candidate.limit_bytes) |limit| {
        if (result.limit_bytes == null or limit < result.limit_bytes.?) {
            result.limit_bytes = limit;
            result.current_bytes = candidate.current_bytes;
        }
    }
    if (candidate.available_bytes) |available| {
        result.available_bytes = if (result.available_bytes) |existing|
            @min(existing, available)
        else
            available;
    }
}

fn mergeAuthoritativeCgroupProbe(
    result: *CgroupMemoryInfo,
    probe: CgroupHierarchyProbe,
) void {
    if (!probe.leaf_present) return;
    mergeCgroupMemoryInfo(result, probe.info);
}

fn probeCgroupMountInfoLine(
    line: []const u8,
    paths: CgroupPaths,
    result: *CgroupMemoryInfo,
) void {
    // Parse one entry in isolation so every visible bind/controller mount is
    // considered. Keeping only the last cgroup mount can select an unrelated
    // subtree when containers expose multiple views of the hierarchy.
    var mounts = CgroupMounts{};
    parseCgroupMountInfoLine(line, &mounts);
    if (paths.v2) |path| {
        if (mounts.v2.valid()) {
            const probe = readCgroupHierarchy(
                mounts.v2.mountPoint(),
                mounts.v2.root(),
                path,
                "memory.max",
                "memory.current",
                .v2,
            );
            mergeAuthoritativeCgroupProbe(result, probe);
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
                .v1,
            );
            mergeAuthoritativeCgroupProbe(result, probe);
        }
    }
}

fn probeCgroupMountHierarchyLinux(paths: CgroupPaths) CgroupMemoryInfo {
    var result = CgroupMemoryInfo{};
    if (builtin.os.tag != .linux) return result;
    const fd = std.posix.openat(
        std.posix.AT.FDCWD,
        "/proc/self/mountinfo",
        .{ .ACCMODE = .RDONLY, .CLOEXEC = true },
        0,
    ) catch return result;
    defer _ = std.posix.system.close(fd);

    var read_buffer: [4096]u8 = undefined;
    var line_buffer: [8192]u8 = undefined;
    var line_len: usize = 0;
    var discard_line = false;
    while (true) {
        const count = std.posix.read(fd, read_buffer[0..]) catch return result;
        if (count == 0) break;
        for (read_buffer[0..count]) |byte| {
            if (byte == '\n') {
                if (!discard_line and line_len > 0)
                    probeCgroupMountInfoLine(line_buffer[0..line_len], paths, &result);
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
        probeCgroupMountInfoLine(line_buffer[0..line_len], paths, &result);
    return result;
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
    inactive_file_bytes: ?usize,
) void {
    const finite_limit = limit orelse return;
    // cgroup v1 uses a near-max integer as its unlimited sentinel.
    if (finite_limit >= std.math.maxInt(usize) / 2) return;
    if (result.limit_bytes == null or finite_limit < result.limit_bytes.?) {
        result.limit_bytes = finite_limit;
        result.current_bytes = current;
    }
    if (current) |usage| {
        const bounded_usage = @min(usage, finite_limit);
        // memory.current and memory.usage_in_bytes include file-backed cache.
        // The kernel can reclaim inactive file pages under cgroup pressure, so
        // treating all of them as permanently resident makes a build-heavy
        // container appear full even after the compiler exits. Use the same
        // conservative working-set convention as container monitoring: only
        // inactive_file is reclaimable, and never more than current usage.
        const reclaimable = @min(
            inactive_file_bytes orelse 0,
            bounded_usage,
        );
        const available = finite_limit -| (bounded_usage - reclaimable);
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
    version: CgroupVersion,
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
    var stat_path_buffer: [std.fs.max_path_bytes]u8 = undefined;
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
        const stat_path = std.fmt.bufPrint(
            &stat_path_buffer,
            "{s}/memory.stat",
            .{directory},
        ) catch break;
        const limit = readLinuxUnsignedFile(limit_path);
        const current = readLinuxUnsignedFile(current_path);
        const inactive_file_bytes = readCgroupInactiveFileBytes(
            stat_path,
            version,
        );
        if (directory.len == leaf_directory_len) {
            result.leaf_present = limit != null or current != null;
            result.info.leaf_current_bytes = current;
        }
        accumulateCgroupLevel(
            &result.info,
            limit,
            current,
            inactive_file_bytes,
        );
        if (directory.len == hierarchy_root_len) break;
        const parent = std.fs.path.dirname(directory) orelse break;
        if (parent.len < hierarchy_root_len) break;
        directory = directory_buffer[0..parent.len];
    }
    return result;
}

fn canonicalCgroupProbeIsAuthoritative(probe: CgroupHierarchyProbe) bool {
    // A readable controller file proves that the canonical process directory
    // exists. The hierarchy walk has already inspected every ancestor up to
    // the mount root, so absence of a finite limit is itself authoritative and
    // must not trigger a serialized /proc/self/mountinfo scan per admission.
    return probe.leaf_present;
}

fn probeCgroupMemoryInfoLinux() CgroupMemoryInfo {
    var cgroup_buffer: [4096]u8 = undefined;
    const cgroup_bytes = readSmallLinuxFile("/proc/self/cgroup", &cgroup_buffer) orelse
        return .{};
    const paths = parseCgroupPaths(cgroup_bytes);
    if (paths.v2) |path| {
        const probe = readCgroupHierarchy(
            "/sys/fs/cgroup",
            "/",
            path,
            "memory.max",
            "memory.current",
            .v2,
        );
        if (canonicalCgroupProbeIsAuthoritative(probe)) return probe.info;
    }
    if (paths.v1_memory) |path| {
        const probe = readCgroupHierarchy(
            "/sys/fs/cgroup/memory",
            "/",
            path,
            "memory.limit_in_bytes",
            "memory.usage_in_bytes",
            .v1,
        );
        if (canonicalCgroupProbeIsAuthoritative(probe)) return probe.info;
    }

    // Canonical mounts cover the common path without parsing mountinfo. When
    // they are not authoritative, inspect every visible controller mount and
    // accept only candidates whose addressed process leaf actually exists.
    return probeCgroupMountHierarchyLinux(paths);
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

fn probeSystemMemoryInfoLinuxSample() LinuxSystemMemorySample {
    if (builtin.os.tag != .linux) return .{};
    const cgroup = probeCgroupMemoryInfoLinux();
    var meminfo_buffer: [8192]u8 = undefined;
    const bytes = readSmallLinuxFile("/proc/meminfo", &meminfo_buffer) orelse
        return .{ .cgroup = cgroup };
    const host = parseLinuxMemInfo(bytes) orelse return .{ .cgroup = cgroup };
    return .{
        .info = applyCgroupMemoryInfo(host, cgroup),
        .cgroup = cgroup,
    };
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

fn saturatingAdd(a: usize, b: usize) usize {
    return std.math.add(usize, a, b) catch std.math.maxInt(usize);
}

fn saturatingMul(a: usize, b: usize) usize {
    return std.math.mul(usize, a, b) catch std.math.maxInt(usize);
}

fn pageAlignTokens(tokens: usize) usize {
    const nonzero = @max(tokens, 1);
    const remainder = nonzero % 16;
    if (remainder == 0) return nonzero;
    return std.math.add(usize, nonzero, 16 - remainder) catch std.math.maxInt(usize);
}

fn geometricTokenCapacity(tokens: usize) usize {
    const aligned = pageAlignTokens(tokens);
    return pageAlignTokens(saturatingAdd(aligned, aligned / 2 + aligned % 2));
}

fn geometricTokenCapacityChecked(tokens: usize) !usize {
    const aligned = try alignForwardChecked(@max(tokens, 1), 16);
    const growth = try std.math.add(usize, aligned / 2, aligned % 2);
    return alignForwardChecked(try std.math.add(usize, aligned, growth), 16);
}

fn generationMaxInflightTokens(prefill_chunk_size: usize) usize {
    // Generation accepts speculative_k through 16 and writes the proposal
    // rows plus the verification row into the same live KV span.
    return @max(@max(prefill_chunk_size, 1), 17);
}

pub const GenerationKvCapacityPolicy = enum {
    full_history,
    split_swa_ring,
};

pub fn estimateGptGeneration(
    backend: kv_pool.BackendKind,
    kv_dtype: kv_pool.KvDType,
    config: gpt_mod.Config,
    prompt_tokens: usize,
    max_tokens: usize,
    prefill_chunk_size: usize,
) EstimateError!Estimate {
    return estimateGptGenerationForKvPolicy(
        backend,
        kv_dtype,
        config,
        prompt_tokens,
        max_tokens,
        prefill_chunk_size,
        .full_history,
    );
}

pub fn estimateGptGenerationForKvPolicy(
    backend: kv_pool.BackendKind,
    kv_dtype: kv_pool.KvDType,
    config: gpt_mod.Config,
    prompt_tokens: usize,
    max_tokens: usize,
    prefill_chunk_size: usize,
    kv_capacity_policy: GenerationKvCapacityPolicy,
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
    const retained_tokens = if (config.kvPoolSlidingWindowSize(false)) |window|
        @min(total_tokens, @as(usize, @intCast(window)))
    else
        total_tokens;
    const page_aligned_tokens = alignForwardChecked(@max(retained_tokens, 1), 16) catch
        return error.ResourceLimitExceeded;
    const metal_capacity_tokens = geometricTokenCapacityChecked(total_tokens) catch
        return error.ResourceLimitExceeded;
    const split_gemma_kv = kv_capacity_policy == .split_swa_ring and
        backend == .metal and
        config.supportsSplitSwaGlobalKvRing();
    // KvPoolConfig has one row geometry for every packed layer. Budget that
    // exact model-wide geometry even when individual layers expose narrower
    // local-attention heads.
    const pool_kv_heads = config.maxKvHeads();
    const pool_head_dim = try estimateMaxHeadDim(config);
    if (pool_kv_heads == 0 or pool_head_dim == 0) return error.InvalidModelConfig;
    const kv_pair_bytes = kv_dtype.bytesForTokenPairChecked(pool_kv_heads, pool_head_dim) catch
        return error.ResourceLimitExceeded;
    var kv_bytes: usize = 0;
    for (0..@as(usize, @intCast(config.num_hidden_layers))) |layer| {
        if (config.layerSharesKv(layer)) continue;
        var layer_capacity_tokens = if (backend == .metal) metal_capacity_tokens else page_aligned_tokens;
        if (split_gemma_kv and config.layerUsesSlidingAttention(layer)) {
            const ring_pages = storage_runtime.swaRingPageCount(
                generation_kv_page_size_tokens,
                config.sliding_window,
                generationMaxInflightTokens(prefill_chunk_size),
                true,
            ) catch return error.ResourceLimitExceeded;
            const ring_tokens = std.math.mul(usize, ring_pages, generation_kv_page_size_tokens) catch
                return error.ResourceLimitExceeded;
            layer_capacity_tokens = ring_tokens;
        }
        const layer_bytes = std.math.mul(usize, layer_capacity_tokens, kv_pair_bytes) catch
            return error.ResourceLimitExceeded;
        kv_bytes = std.math.add(usize, kv_bytes, layer_bytes) catch
            return error.ResourceLimitExceeded;
    }

    const scratch_rows = @min(@max(prompt_tokens, 1), @max(prefill_chunk_size, 1));
    const hidden = @as(usize, @intCast(config.hidden_size));
    const heads = @as(usize, @intCast(config.num_attention_heads));
    const head_dim = @as(usize, try estimateMaxHeadDim(config));
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
    const base_scratch_bytes = std.math.add(usize, activation_scratch, logits_scratch) catch
        return error.ResourceLimitExceeded;
    // CUDA Gemma4 prefill enqueues layer temporaries asynchronously. Until the
    // allocator exposes an admission-grade physical high-water, budget one
    // maximum FFN activation per layer: a measured 2051-row E2B prefill
    // otherwise underestimates scratch by roughly 2 GiB and forces a device
    // synchronization to drain deferred frees. This envelope is deliberately
    // CUDA/Gemma/PLE-specific so it does not undo the independently qualified
    // Metal chunk policy.
    const cuda_deferred_activation_scratch = if (backend == .cuda and
        config.family == .gemma and config.hasPle())
    blk: {
        var max_intermediate: usize = 0;
        for (0..@as(usize, @intCast(config.num_hidden_layers))) |layer| {
            max_intermediate = @max(
                max_intermediate,
                @as(usize, @intCast(config.intermediateSize(layer))),
            );
        }
        break :blk checkedProduct(&.{
            scratch_rows,
            max_intermediate,
            @as(usize, @intCast(config.num_hidden_layers)),
            @sizeOf(f32),
        }) catch return error.ResourceLimitExceeded;
    } else 0;
    const scratch_bytes = std.math.add(
        usize,
        base_scratch_bytes,
        cuda_deferred_activation_scratch,
    ) catch return error.ResourceLimitExceeded;

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

pub const GptGenerationBudgetComponent = struct {
    backend: kv_pool.BackendKind,
    kv_dtype: kv_pool.KvDType,
    config: gpt_mod.Config,
    kv_capacity_policy: GenerationKvCapacityPolicy = .full_history,
};

pub const GptGenerationPrefillAdmission = struct {
    max_chunk_rows: usize,
    max_scratch_bytes: usize,
};

/// Reserves target and optional draft generation memory at the largest chunk
/// that fits. Failed attempts are isolated, so only the selected geometry is
/// committed to the caller's budget.
pub fn reserveGptGenerationAtLargestChunk(
    budget: *RunBudget,
    components: []const GptGenerationBudgetComponent,
    prompt_tokens: usize,
    max_tokens: usize,
    chunk_ceiling: usize,
) !usize {
    return (try reserveGptGenerationPrefill(
        budget,
        components,
        prompt_tokens,
        max_tokens,
        chunk_ceiling,
    )).max_chunk_rows;
}

/// Reserve generation resources and return the complete metadata needed to
/// fingerprint an immutable prefill plan. `max_scratch_bytes` is the request's
/// incremental scratch reservation, excluding reservations already present on
/// the supplied run budget.
pub fn reserveGptGenerationPrefill(
    budget: *RunBudget,
    components: []const GptGenerationBudgetComponent,
    prompt_tokens: usize,
    max_tokens: usize,
    chunk_ceiling: usize,
) !GptGenerationPrefillAdmission {
    const scratch_before = budget.scratchTotalBytes();
    var chunk = @max(chunk_ceiling, 1);
    while (true) {
        var trial = budget.*;
        var failed = false;
        for (components) |component| {
            trial.reserveEstimate(try estimateGptGenerationForKvPolicy(
                component.backend,
                component.kv_dtype,
                component.config,
                prompt_tokens,
                max_tokens,
                chunk,
                component.kv_capacity_policy,
            )) catch |err| {
                if (err != error.MemoryBudgetExceeded) return err;
                failed = true;
                break;
            };
        }
        if (!failed) {
            const selected_scratch = trial.scratchTotalBytes() -| scratch_before;
            budget.* = trial;
            return .{
                .max_chunk_rows = chunk,
                .max_scratch_bytes = selected_scratch,
            };
        }
        if (chunk <= 32) {
            const denial_count = trial.denials;
            const last_denial = trial.last_denial;
            budget.denials = denial_count;
            budget.last_denial = last_denial;
            return error.MemoryBudgetExceeded;
        }
        const power_of_two_ceiling = std.math.ceilPowerOfTwo(usize, chunk) catch chunk;
        chunk = @max(power_of_two_ceiling / 2, 32);
    }
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

test "cgroup memory stat parser uses hierarchical inactive file cache" {
    const stat =
        \\anon 1048576
        \\file 8388608
        \\inactive_file 4194304
        \\total_inactive_file 6291456
        \\
    ;
    try std.testing.expectEqual(
        @as(?usize, 4194304),
        parseCgroupInactiveFileBytes(stat, .v2),
    );
    try std.testing.expectEqual(
        @as(?usize, 6291456),
        parseCgroupInactiveFileBytes(stat, .v1),
    );
    try std.testing.expectEqual(
        @as(?usize, 4194304),
        parseCgroupInactiveFileBytes("inactive_file 4194304\n", .v1),
    );
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
    const dotted = parseCgroupPaths(
        \\0::/system.slice/worker..scope
        \\
    );
    try std.testing.expectEqualStrings("/system.slice/worker..scope", dotted.v2.?);
    try std.testing.expect(parseCgroupPaths("0::/safe/../escape\n").v2 == null);

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
    try std.testing.expect(canonicalCgroupProbeIsAuthoritative(.{
        .info = .{},
        .leaf_present = true,
    }));
}

test "cgroup mount candidates ignore ancestor-only probes" {
    var result = CgroupMemoryInfo{};
    const ancestor_only = CgroupHierarchyProbe{
        .info = .{
            .limit_bytes = gib(8),
            .current_bytes = gib(6),
            .available_bytes = gib(2),
        },
        .leaf_present = false,
    };
    mergeAuthoritativeCgroupProbe(&result, ancestor_only);
    try std.testing.expectEqual(@as(?usize, null), result.limit_bytes);
    try std.testing.expectEqual(@as(?usize, null), result.available_bytes);

    mergeCgroupMemoryInfo(&result, .{
        .limit_bytes = gib(2),
        .current_bytes = gib(1),
        .available_bytes = gib(1),
    });
    mergeCgroupMemoryInfo(&result, .{
        .limit_bytes = gib(4),
        .current_bytes = gib(1),
        .available_bytes = gib(3),
    });
    try std.testing.expectEqual(@as(?usize, gib(2)), result.limit_bytes);
    try std.testing.expectEqual(@as(?usize, gib(1)), result.available_bytes);
}

test "cgroup hierarchy uses finite parent limit beneath unlimited leaf" {
    var hierarchy = CgroupMemoryInfo{};
    // A v2 "max" leaf is represented as null and must not stop the ancestor walk.
    accumulateCgroupLevel(&hierarchy, null, gib(3), null);
    accumulateCgroupLevel(&hierarchy, gib(8), gib(6), null);
    accumulateCgroupLevel(&hierarchy, gib(16), gib(10), null);

    try std.testing.expectEqual(@as(?usize, gib(8)), hierarchy.limit_bytes);
    try std.testing.expectEqual(@as(?usize, gib(2)), hierarchy.available_bytes);
    const effective = applyCgroupMemoryInfo(
        .{ .total_bytes = gib(64), .available_bytes = gib(32) },
        hierarchy,
    );
    try std.testing.expectEqual(gib(8), effective.total_bytes);
    try std.testing.expectEqual(@as(?usize, gib(2)), effective.available_bytes);
}

test "cgroup availability includes only reclaimable inactive file cache" {
    var hierarchy = CgroupMemoryInfo{};
    accumulateCgroupLevel(
        &hierarchy,
        gib(64),
        gib(47),
        gib(30),
    );
    try std.testing.expectEqual(@as(?usize, gib(47)), hierarchy.available_bytes);

    const effective = applyCgroupMemoryInfo(
        .{ .total_bytes = gib(128), .available_bytes = gib(80) },
        hierarchy,
    );
    try std.testing.expectEqual(gib(64), effective.total_bytes);
    try std.testing.expectEqual(@as(?usize, gib(47)), effective.available_bytes);

    var saturated = CgroupMemoryInfo{};
    accumulateCgroupLevel(&saturated, gib(8), gib(7), gib(12));
    try std.testing.expectEqual(@as(?usize, gib(8)), saturated.available_bytes);
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
    try checkLiveHostMemoryWithInfo(
        .{ .total_bytes = gib(2), .available_bytes = gib(1) },
        mib(128),
    );
}

test "explicit process envelope charges raw leaf cgroup usage" {
    const info = applyProcessMemoryLimit(
        .{
            .total_bytes = gib(64),
            .available_bytes = gib(40),
            .availability_basis = .mem_available,
        },
        gib(14),
        gib(3),
    );
    try std.testing.expectEqual(gib(14), info.total_bytes);
    try std.testing.expectEqual(@as(?usize, gib(11)), info.available_bytes);

    const saturated = applyProcessMemoryLimit(info, gib(14), gib(20));
    try std.testing.expectEqual(@as(?usize, 0), saturated.available_bytes);

    const clamped = applyProcessMemoryLimit(
        .{ .total_bytes = gib(8), .available_bytes = gib(6) },
        gib(14),
        gib(2),
    );
    try std.testing.expectEqual(gib(8), clamped.total_bytes);
    try std.testing.expectEqual(@as(?usize, gib(6)), clamped.available_bytes);
}

test "explicit process envelope remains authoritative without a host probe" {
    const live = applyOptionalProcessMemoryLimit(null, gib(14), gib(3)).?;
    try std.testing.expectEqual(gib(14), live.total_bytes);
    try std.testing.expectEqual(@as(?usize, gib(11)), live.available_bytes);

    const stable = stableMemoryInfoForLimit(null, gib(14)).?;
    try std.testing.expectEqual(gib(14), stable.total_bytes);
    try std.testing.expectEqual(@as(?usize, gib(14)), stable.available_bytes);
    try std.testing.expect(stableMemoryInfoForLimit(null, 0) == null);
}

test "Linux live admission admits the CI model under shared-host pressure" {
    // Reproduce the failing runner sample exactly: the old live gate retained
    // the full 15.7 GiB node headroom a second time, leaving only 1.0 GiB for a
    // 3.96 GiB model despite 16.7 GiB being available.
    const info = SystemMemoryInfo{
        .total_bytes = 67_428_634_624,
        .available_bytes = 17_917_997_056,
        .availability_basis = .mem_available,
    };

    try std.testing.expectEqual(@as(usize, 8_958_998_528), liveHostAdmissionReserve(info));
    try checkLiveHostMemoryWithInfo(info, 4_248_561_560);
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        checkLiveHostMemoryWithInfo(info, 8_958_998_529),
    );

    const later_request = SystemMemoryInfo{
        .total_bytes = info.total_bytes,
        // Reproduce the same runner after the model is resident. The prior
        // policy reserved this entire sample and rejected even 3.4 MiB of
        // request scratch despite 5.4 GiB remaining available.
        .available_bytes = 5_775_224_832,
        .availability_basis = .mem_available,
    };
    try std.testing.expectEqual(@as(usize, 2_887_612_416), liveHostAdmissionReserve(later_request));
    try checkLiveHostMemoryWithInfo(later_request, 3_358_720);

    const emergency_pressure = SystemMemoryInfo{
        .total_bytes = info.total_bytes,
        .available_bytes = mib(512),
        .availability_basis = .mem_available,
    };
    try std.testing.expectEqual(mib(512), liveHostAdmissionReserve(emergency_pressure));
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        checkLiveHostMemoryWithInfo(emergency_pressure, 1),
    );
}

test "macOS live admission does not double reserve stable node headroom" {
    const info = SystemMemoryInfo{
        .total_bytes = gib(36),
        // Raw free (which includes speculative) + inactive pages observed on a
        // healthy host.
        // The full stable node headroom is 9 GiB, but macOS still reported
        // normal pressure because compressed memory remained reclaimable.
        .available_bytes = mib(8480),
        .availability_basis = .macos_reclaimable_pages,
    };

    try std.testing.expectEqual(mib(7328), liveHostAdmissionReserve(info));
    try checkLiveHostMemoryWithInfo(info, mib(256));

    var controller = AdmissionController{};
    _ = try controller.tryReserveLiveCapacityWithInfo(mib(256), info);
    try std.testing.expectEqual(mib(1152), controller.live_capacity_bytes);
    controller.releaseLiveReservation(mib(256));
}

test "macOS live admission bounds grace capacity for large loads" {
    const info = SystemMemoryInfo{
        .total_bytes = gib(36),
        .available_bytes = mib(8480),
        .availability_basis = .macos_reclaimable_pages,
    };

    try checkLiveHostMemoryWithInfo(info, mib(1152));
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        checkLiveHostMemoryWithInfo(info, mib(1153)),
    );
}

test "macOS reclaimable pages do not double count speculative pages" {
    try std.testing.expectEqual(
        @as(u64, 300),
        macosReclaimablePageCount(100, 200, 50),
    );
}

test "macOS memory pressure availability includes reclaimable cache" {
    try std.testing.expectEqual(
        @as(?u64, 12 * 1024 * 1024 * 1024),
        macosMemoryPressureAvailableBytes(24 * 1024 * 1024 * 1024, 50),
    );
    try std.testing.expectEqual(
        @as(?u64, null),
        macosMemoryPressureAvailableBytes(24 * 1024 * 1024 * 1024, 101),
    );
}

test "macOS system memory probe reports bounded availability" {
    if (builtin.os.tag != .macos) return error.SkipZigTest;
    const info = probeSystemMemoryInfoMacos() orelse return error.TestUnexpectedResult;
    const available = info.available_bytes orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(AvailabilityBasis.macos_memory_pressure, info.availability_basis);
    try std.testing.expect(available <= info.total_bytes);
}

test "macOS live admission preserves an emergency reserve under scarcity" {
    const info = SystemMemoryInfo{
        .total_bytes = gib(36),
        .available_bytes = mib(640),
        .availability_basis = .macos_reclaimable_pages,
    };

    try std.testing.expectEqual(mib(512), liveHostAdmissionReserve(info));
    try checkLiveHostMemoryWithInfo(info, mib(128));
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        checkLiveHostMemoryWithInfo(info, mib(129)),
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
        .cpu,
        cpu,
        .{ .host_weight_bytes = mib(600) },
        false,
    );
    defer first.release();
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        controller.tryAcquire(
            .cpu,
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
    var first = try controller.tryAcquire(.cpu, limits, .{ .host_weight_bytes = 60 }, false);
    defer first.release();
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        controller.tryAcquire(.cpu, limits, .{ .host_kv_bytes = 50 }, false),
    );
    first.release();
    var second = try controller.tryAcquire(.cpu, limits, .{ .host_kv_bytes = 50 }, false);
    defer second.release();
    try std.testing.expectEqual(@as(usize, 50), controller.snapshot().hostTotalBytes());
}

test "admission reports the exact transient pressure domain" {
    var controller = AdmissionController{};
    var scratch_lease = try controller.tryAcquire(
        .cpu,
        .{ .scratch_limit_bytes = 100 },
        .{ .host_scratch_bytes = 70 },
        false,
    );
    defer scratch_lease.release();

    var pressure: ?AdmissionPressure = null;
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        controller.tryAcquireWithPressure(
            .cpu,
            .{ .scratch_limit_bytes = 100 },
            .{ .host_scratch_bytes = 40 },
            false,
            &pressure,
        ),
    );
    switch (pressure orelse return error.TestExpectedAdmissionPressure) {
        .domain_scratch => |backend_class| try std.testing.expectEqual(BackendClass.cpu, backend_class),
        else => return error.TestUnexpectedAdmissionPressure,
    }

    scratch_lease.release();
    var backend_lease = try controller.tryAcquire(
        .gpu,
        .{ .backend_limit_bytes = 100 },
        .{ .backend_weight_bytes = 70 },
        false,
    );
    defer backend_lease.release();
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        controller.tryAcquireWithPressure(
            .gpu,
            .{ .backend_limit_bytes = 100 },
            .{ .backend_weight_bytes = 40 },
            false,
            &pressure,
        ),
    );
    switch (pressure orelse return error.TestExpectedAdmissionPressure) {
        .domain_backend => |backend_class| try std.testing.expectEqual(BackendClass.gpu, backend_class),
        else => return error.TestUnexpectedAdmissionPressure,
    }
}

test "cpu and gpu admission enforce independent policy domains" {
    var controller = AdmissionController{};
    controller.configureSharedLimits(.{ .host_limit_bytes = 100 });
    var gpu_lease = try controller.tryAcquire(
        .gpu,
        .{
            .backend_limit_bytes = 1000,
            .combined_limit_bytes = 1000,
            .kv_limit_bytes = 1000,
        },
        .{ .backend_kv_bytes = 800 },
        false,
    );
    defer gpu_lease.release();

    // A discrete-GPU reservation is still visible in the aggregate snapshot,
    // but it must not consume the CPU policy's much smaller combined/KV caps.
    var cpu_lease = try controller.tryAcquire(
        .cpu,
        .{
            .host_limit_bytes = 100,
            .combined_limit_bytes = 100,
            .kv_limit_bytes = 100,
        },
        .{ .host_kv_bytes = 40 },
        false,
    );
    defer cpu_lease.release();

    try std.testing.expectEqual(@as(usize, 840), controller.snapshot().kvTotalBytes());
    try std.testing.expectEqual(
        @as(usize, 40),
        controller.snapshotBackend(.cpu).kvTotalBytes(),
    );
    try std.testing.expectEqual(
        @as(usize, 800),
        controller.snapshotBackend(.gpu).kvTotalBytes(),
    );
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        controller.tryAcquire(
            .cpu,
            .{
                .host_limit_bytes = 100,
                .combined_limit_bytes = 100,
                .kv_limit_bytes = 100,
            },
            .{ .host_kv_bytes = 70 },
            false,
        ),
    );
}

test "cpu and gpu admission share one physical host-memory domain" {
    var controller = AdmissionController{};
    controller.configureSharedLimits(.{ .host_limit_bytes = 100 });
    const limits = Limits{
        .host_limit_bytes = 100,
        .combined_limit_bytes = 100,
    };

    var cpu_lease = try controller.tryAcquire(
        .cpu,
        limits,
        .{ .host_weight_bytes = 60 },
        false,
    );
    defer cpu_lease.release();

    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        controller.tryAcquire(
            .gpu,
            limits,
            .{ .host_scratch_bytes = 50 },
            false,
        ),
    );
    try std.testing.expectEqual(@as(usize, 60), controller.snapshot().hostTotalBytes());
    try std.testing.expectEqual(
        AdmissionAmounts{},
        controller.snapshotBackend(.gpu),
    );

    // Device-local CUDA bytes remain independent and do not consume the shared
    // host cap or the CPU workload policy.
    var gpu_lease = try controller.tryAcquire(
        .gpu,
        .{
            .host_limit_bytes = 100,
            .backend_limit_bytes = 1000,
            .combined_limit_bytes = 1000,
        },
        .{ .backend_weight_bytes = 800 },
        false,
    );
    defer gpu_lease.release();
    try std.testing.expectEqual(@as(usize, 60), controller.snapshot().hostTotalBytes());
    try std.testing.expectEqual(@as(usize, 800), controller.snapshot().backendTotalBytes());
}

test "live memory admissions share one sampled capacity epoch" {
    var controller = AdmissionController{};
    const info = SystemMemoryInfo{
        .total_bytes = gib(64),
        .available_bytes = gib(20),
    };

    _ = try controller.tryReserveLiveCapacityWithInfo(gib(7), info);
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        controller.tryReserveLiveCapacityWithInfo(gib(4), info),
    );
    try std.testing.expectEqual(gib(7), controller.live_pending_bytes);
    try std.testing.expectEqual(gib(10), controller.live_capacity_bytes);

    controller.releaseLiveReservation(gib(7));
    _ = try controller.tryReserveLiveCapacityWithInfo(gib(2), info);
    controller.releaseLiveReservation(gib(2));
    try std.testing.expectEqual(@as(usize, 0), controller.live_pending_bytes);
    try std.testing.expectEqual(@as(usize, 0), controller.live_capacity_bytes);
}

test "settled live residency remains committed during pressure epoch" {
    var controller = AdmissionController{};
    const info = SystemMemoryInfo{
        .total_bytes = gib(64),
        .available_bytes = gib(20),
    };

    _ = try controller.tryReserveLiveCapacityWithInfo(gib(3), info);
    _ = try controller.tryReserveLiveCapacityWithInfo(gib(1), info);
    controller.settleLiveReservation(gib(3), gib(2));

    try std.testing.expectEqual(gib(1), controller.live_pending_bytes);
    try std.testing.expectEqual(gib(8), controller.live_capacity_bytes);
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        controller.tryReserveLiveCapacityWithInfo(gib(8), info),
    );

    controller.releaseLiveReservation(gib(1));
    try std.testing.expectEqual(@as(usize, 0), controller.live_pending_bytes);
    try std.testing.expectEqual(@as(usize, 0), controller.live_capacity_bytes);
}

test "metal backend bytes share the unified system-memory domain" {
    if (builtin.os.tag != .macos) return error.SkipZigTest;

    var controller = AdmissionController{};
    controller.configureSharedLimits(.{
        .host_limit_bytes = 1000,
        .unified_limit_bytes = 100,
    });
    var metal_lease = try controller.tryAcquire(
        .gpu,
        .{
            .backend_limit_bytes = 100,
            .combined_limit_bytes = 100,
        },
        .{ .backend_weight_bytes = 60 },
        false,
    );
    defer metal_lease.release();

    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        controller.tryAcquire(
            .cpu,
            .{
                .host_limit_bytes = 100,
                .combined_limit_bytes = 100,
            },
            .{ .host_weight_bytes = 50 },
            false,
        ),
    );
    try std.testing.expectEqual(@as(usize, 60), controller.snapshot().backendTotalBytes());
    try std.testing.expectEqual(
        AdmissionAmounts{},
        controller.snapshotBackend(.cpu),
    );
}

test "multi-domain admission is atomic and uses each domain limits" {
    var controller = AdmissionController{};
    try std.testing.expectError(
        error.ResourceLimitExceeded,
        controller.tryAcquireRequests(
            &.{
                .{
                    .backend_class = .cpu,
                    .limits = .{ .combined_limit_bytes = 100 },
                    .amounts = .{ .host_kv_bytes = 60 },
                },
                .{
                    .backend_class = .gpu,
                    .limits = .{ .combined_limit_bytes = 100 },
                    .amounts = .{ .backend_kv_bytes = 101 },
                },
            },
            false,
        ),
    );
    try std.testing.expectEqual(AdmissionAmounts{}, controller.snapshot());
    try std.testing.expectEqual(AdmissionAmounts{}, controller.snapshotBackend(.cpu));
    try std.testing.expectEqual(AdmissionAmounts{}, controller.snapshotBackend(.gpu));
}

test "admission lease releases transient construction bytes while retaining residency" {
    var controller = AdmissionController{};
    var lease = try controller.tryAcquire(
        .gpu,
        .{
            .host_limit_bytes = 200,
            .backend_limit_bytes = 200,
            .combined_limit_bytes = 400,
        },
        .{
            .host_weight_bytes = 120,
            .backend_weight_bytes = 180,
        },
        false,
    );
    defer lease.release();
    try lease.retain(.{ .backend_weight_bytes = 180 });

    const retained = controller.snapshot();
    try std.testing.expectEqual(@as(usize, 0), retained.host_weight_bytes);
    try std.testing.expectEqual(@as(usize, 180), retained.backend_weight_bytes);
    try std.testing.expectError(
        error.InvalidAdmissionLeaseReduction,
        lease.retain(.{ .backend_weight_bytes = 181 }),
    );
}

test "admission resource budget mirrors acquire retain and release" {
    const Recorder = struct {
        current: AdmissionAmounts = .{},
        references: usize = 1,
        controller: ?*AdmissionController = null,
        reconfiguration_blocked_during_release: bool = false,

        fn retainContext(context: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.references += 1;
            return true;
        }

        fn releaseContext(context: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            std.debug.assert(self.references > 1);
            self.references -= 1;
        }

        fn reserve(context: *anyopaque, amounts: AdmissionAmounts) AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.current = try self.current.merge(amounts);
            return 1;
        }

        fn retain(context: *anyopaque, lease: usize, retained: AdmissionAmounts) AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (lease != 1) return error.ResourceLimitExceeded;
            self.current = retained;
        }

        fn release(context: *anyopaque, lease: usize) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            std.debug.assert(lease == 1);
            if (self.controller) |controller| {
                controller.configureResourceBudget(null) catch |err| {
                    self.reconfiguration_blocked_during_release =
                        err == error.ResourceBudgetInUse;
                    self.current = .{};
                    return;
                };
                self.reconfiguration_blocked_during_release = false;
            }
            self.current = .{};
        }
    };

    var recorder = Recorder{};
    var controller = AdmissionController{};
    recorder.controller = &controller;
    try controller.configureResourceBudget(.{
        .context = &recorder,
        .retain_context = Recorder.retainContext,
        .release_context = Recorder.releaseContext,
        .try_reserve = Recorder.reserve,
        .retain = Recorder.retain,
        .release = Recorder.release,
    });
    try std.testing.expectEqual(@as(usize, 2), recorder.references);
    var lease = try controller.tryAcquire(.cpu, .{}, .{
        .host_weight_bytes = 64,
        .host_scratch_bytes = 32,
    }, false);
    try std.testing.expectEqual(@as(usize, 64), recorder.current.host_weight_bytes);
    try std.testing.expectEqual(@as(usize, 32), recorder.current.host_scratch_bytes);
    try std.testing.expectError(
        error.ResourceBudgetInUse,
        controller.configureResourceBudget(null),
    );
    try std.testing.expectEqual(@as(usize, 2), recorder.references);

    try lease.retain(.{ .host_weight_bytes = 64 });
    try std.testing.expectEqual(@as(usize, 0), recorder.current.host_scratch_bytes);
    lease.release();
    try std.testing.expect(recorder.reconfiguration_blocked_during_release);
    try std.testing.expectEqual(@as(usize, 2), recorder.references);
    try std.testing.expectEqual(AdmissionAmounts{}, recorder.current);
    controller.deinit();
    try std.testing.expectEqual(@as(usize, 1), recorder.references);
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
        controller.tryAcquire(.cpu, .{ .host_limit_bytes = 100 }, .{ .host_weight_bytes = 101 }, false),
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
    try std.testing.expectEqual(@as(usize, 176), estimate.kv_bytes / (32 * 8 * 128 * 2 * 2));
    try std.testing.expectEqual(ResidencyTier.backend, estimate.kv_tier);
    try std.testing.expect(estimate.scratch_bytes > 0);

    // int8: bytesForTokenRow(8, 128) = 1024 + 8*4 = 1056
    const est_int8 = try estimateGptGeneration(.metal, .int8, cfg, 100, 10, 64);
    try std.testing.expectEqual(@as(usize, 176 * 32 * 1056 * 2), est_int8.kv_bytes);

    // int4: bytesForTokenRow(8, 128) = ceil(1024/32)*18 = 32*18 = 576
    const est_int4 = try estimateGptGeneration(.metal, .int4, cfg, 100, 10, 64);
    try std.testing.expectEqual(@as(usize, 176 * 32 * 576 * 2), est_int4.kv_bytes);
}

test "generation token alignment saturates instead of wrapping below the request" {
    try std.testing.expectEqual(@as(usize, 16), pageAlignTokens(1));
    try std.testing.expectEqual(@as(usize, 32), pageAlignTokens(17));
    try std.testing.expectEqual(std.math.maxInt(usize), pageAlignTokens(std.math.maxInt(usize)));
    try std.testing.expectEqual(std.math.maxInt(usize), geometricTokenCapacity(std.math.maxInt(usize)));
}

test "generation ring estimate covers the maximum speculative verification span" {
    try std.testing.expectEqual(@as(usize, 17), generationMaxInflightTokens(1));
    try std.testing.expectEqual(@as(usize, 17), generationMaxInflightTokens(16));
    try std.testing.expectEqual(@as(usize, 128), generationMaxInflightTokens(128));
}

test "gpt generation estimate keeps mixed Gemma global history" {
    const cfg = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 2560,
        .num_hidden_layers = 42,
        .num_attention_heads = 8,
        .num_key_value_heads = 2,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .vocab_size = 262144,
        .sliding_window = 512,
        .sliding_window_pattern = 6,
        .num_kv_shared_layers = 18,
        .ple_hidden_size = 2560,
        .position_encoding = .rope,
    };

    const estimate = try estimateGptGeneration(.metal, .f16, cfg, 2000, 100, 512);
    try std.testing.expectEqual(@as(usize, 2100), estimate.retained_tokens);
    // The packed pool uses the maximum 2 x 512 row geometry for all 24 donor
    // layers and budgets geometric Metal growth. Shared tail layers do not
    // own KV storage.
    try std.testing.expectEqual(@as(usize, 311_427_072), estimate.kv_bytes);
    try std.testing.expectEqual(@as(usize, 76_546_048), estimate.scratch_bytes);

    const ring_estimate = try estimateGptGenerationForKvPolicy(
        .metal,
        .f16,
        cfg,
        2000,
        100,
        512,
        .split_swa_ring,
    );
    // Four global donor layers retain geometric full history. The other 20
    // donor layers use the 1,040-token ring actually allocated at runtime.
    try std.testing.expectEqual(@as(usize, 137_101_312), ring_estimate.kv_bytes);
    try std.testing.expect(ring_estimate.kv_bytes < estimate.kv_bytes);

    const long_full = try estimateGptGeneration(.metal, .f16, cfg, 65_536, 300, 256);
    const long_ring = try estimateGptGenerationForKvPolicy(
        .metal,
        .f16,
        cfg,
        65_536,
        300,
        256,
        .split_swa_ring,
    );
    try std.testing.expect(long_full.kv_bytes > gib(3));
    try std.testing.expect(long_ring.kv_bytes < gib(3));
}

test "gpt generation estimate covers a sliding ring larger than global growth" {
    const cfg = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 2560,
        .num_hidden_layers = 2,
        .num_attention_heads = 8,
        .num_key_value_heads = 2,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .vocab_size = 1,
        .sliding_window = 512,
        .sliding_window_pattern = 2,
        .ple_hidden_size = 2560,
        .position_encoding = .rope,
    };

    const estimate = try estimateGptGenerationForKvPolicy(
        .metal,
        .f16,
        cfg,
        16,
        1,
        16,
        .split_swa_ring,
    );
    // Local ring: 34 pages = 544 tokens. Global growth: 32 -> 48 tokens. Both
    // layers use the packed pool's maximum 2 x 512 row geometry.
    try std.testing.expectEqual(@as(usize, 2_424_832), estimate.kv_bytes);
}

test "generation budget downshifts target and draft together" {
    const cfg = gpt_mod.Config{
        .hidden_size = 8,
        .num_hidden_layers = 1,
        .num_attention_heads = 1,
        .num_key_value_heads = 1,
        .attention_head_dim = 8,
        .vocab_size = 1,
        .position_encoding = .rope,
    };
    const components = [_]GptGenerationBudgetComponent{
        .{ .backend = .metal, .kv_dtype = .f16, .config = cfg },
        .{ .backend = .metal, .kv_dtype = .f16, .config = cfg },
    };
    var budget = RunBudget.init(.{ .scratch_limit_bytes = 50_000 });

    const selected = try reserveGptGenerationAtLargestChunk(
        &budget,
        &components,
        200,
        1,
        200,
    );
    try std.testing.expectEqual(@as(usize, 64), selected);
    try std.testing.expectEqual(
        2 * (try estimateGptGeneration(.metal, .f16, cfg, 200, 1, 64)).scratch_bytes,
        budget.scratchTotalBytes(),
    );
}

test "cuda Gemma4 prefill rejects unsafe 2051 row physical envelope" {
    const cfg = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 1536,
        .num_hidden_layers = 35,
        .num_attention_heads = 8,
        .num_key_value_heads = 1,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .intermediate_size = 6144,
        .shared_layer_intermediate_size = 12288,
        .num_kv_shared_layers = 20,
        .vocab_size = 262144,
        .sliding_window = 512,
        .sliding_window_pattern = 5,
        .ple_hidden_size = 256,
        .position_encoding = .rope,
    };
    const unsafe = try estimateGptGeneration(.cuda, .f16, cfg, 2051, 300, 2051);
    try std.testing.expect(unsafe.scratch_bytes > 2 * 1024 * 1024 * 1024);

    const components = [_]GptGenerationBudgetComponent{
        .{ .backend = .cuda, .kv_dtype = .f16, .config = cfg },
    };
    var budget = RunBudget.init(.{ .scratch_limit_bytes = 2 * 1024 * 1024 * 1024 });
    const admission = try reserveGptGenerationPrefill(
        &budget,
        &components,
        2051,
        300,
        2051,
    );
    try std.testing.expect(admission.max_chunk_rows < 2051);
    try std.testing.expect(admission.max_scratch_bytes <= 2 * 1024 * 1024 * 1024);
    try std.testing.expectEqual(admission.max_scratch_bytes, budget.scratchTotalBytes());
}

test "cuda Gemma4 measured 512 ceiling returns plan scratch metadata" {
    const cfg = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 1536,
        .num_hidden_layers = 35,
        .num_attention_heads = 8,
        .num_key_value_heads = 1,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .intermediate_size = 6144,
        .shared_layer_intermediate_size = 12288,
        .num_kv_shared_layers = 20,
        .vocab_size = 262144,
        .sliding_window = 512,
        .sliding_window_pattern = 5,
        .ple_hidden_size = 256,
        .position_encoding = .rope,
    };
    const components = [_]GptGenerationBudgetComponent{
        .{ .backend = .cuda, .kv_dtype = .f16, .config = cfg },
    };
    var budget = RunBudget.init(.{ .scratch_limit_bytes = 2 * 1024 * 1024 * 1024 });
    const admission = try reserveGptGenerationPrefill(&budget, &components, 2051, 300, 512);
    try std.testing.expectEqual(@as(usize, 512), admission.max_chunk_rows);
    try std.testing.expect(admission.max_scratch_bytes > 0);
    try std.testing.expectEqual(admission.max_scratch_bytes, budget.scratchTotalBytes());
}

test "generation budget rolls back all components when the minimum chunk fails" {
    const cfg = gpt_mod.Config{
        .hidden_size = 8,
        .num_hidden_layers = 1,
        .num_attention_heads = 1,
        .attention_head_dim = 8,
        .vocab_size = 1,
    };
    const components = [_]GptGenerationBudgetComponent{
        .{ .backend = .native, .kv_dtype = .f16, .config = cfg },
        .{ .backend = .native, .kv_dtype = .f16, .config = cfg },
    };
    const one_component_scratch = (try estimateGptGeneration(.native, .f16, cfg, 200, 1, 32)).scratch_bytes;
    var budget = RunBudget.init(.{ .scratch_limit_bytes = one_component_scratch + 1 });

    try std.testing.expectError(
        error.MemoryBudgetExceeded,
        reserveGptGenerationAtLargestChunk(&budget, &components, 200, 1, 2048),
    );
    try std.testing.expectEqual(@as(usize, 0), budget.kvTotalBytes());
    try std.testing.expectEqual(@as(usize, 0), budget.scratchTotalBytes());
    try std.testing.expect(budget.hasLastDenial());
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
            .cpu,
            .{},
            .{ .host_weight_bytes = std.math.maxInt(usize), .host_kv_bytes = 1 },
            false,
        ),
    );
}

test "admission rejects aggregate overflow across backend domains" {
    var controller = AdmissionController{};
    var cpu_lease = try controller.tryAcquire(
        .cpu,
        .{},
        .{ .host_weight_bytes = std.math.maxInt(usize) },
        false,
    );
    defer cpu_lease.release();

    try std.testing.expectError(
        error.ResourceLimitExceeded,
        controller.tryAcquire(
            .gpu,
            .{},
            .{ .host_kv_bytes = 1 },
            false,
        ),
    );
    try std.testing.expectEqual(
        std.math.maxInt(usize),
        controller.snapshot().host_weight_bytes,
    );
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_kv_bytes);
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
        controller.tryAcquire(.cpu, .{ .combined_limit_bytes = 90 }, combined, false),
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
