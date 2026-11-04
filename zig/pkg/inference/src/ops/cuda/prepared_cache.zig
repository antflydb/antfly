// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One cold phase's exact cache admissions. This owns no CUDA/model/Io/arena.
//! Actual Stage owns snapshots, current epoch and numerical binding. Requests
//! contain values only; a name supplied on a caller stack never escapes.
const std = @import("std");
const platform = @import("antfly_platform");
const memory = @import("../../runtime/tier/memory.zig");
const domain = @import("../../runtime/tier/admission_domain.zig");
const retirement = @import("../../execution/resource_retirement.zig");
const sink = @import("../../execution/retirement_sink.zig");
const policy = @import("video_cache_budget.zig").Admission;
const CacheQuota = @import("cache_quota.zig").Quota;

pub const Name = struct {
    bytes: [256]u8 = @splat(0),
    len: u16,
    pub fn init(text: []const u8) !Name {
        if (text.len == 0 or text.len > 256) return error.InvalidPreparedCacheName;
        var value: Name = .{ .len = @intCast(text.len) };
        @memcpy(value.bytes[0..text.len], text);
        return value;
    }
    pub fn slice(self: *const Name) []const u8 {
        return self.bytes[0..self.len];
    }
};
pub const Resize = struct { width: u32, height: u32, target_width: u32, target_height: u32, resident: bool };
pub const Arena = struct {
    kind: enum { encoder, vision, audio },
    batch: usize,
    sequence: usize,
    mask_kind: u8 = 0,
    grid_x: usize = 0,
    grid_y: usize = 0,
    sizes: [11]usize,
};
/// Exact immutable BF16 concatenation, in the producer's original copy order.
/// Only numeric identities are copied; no borrowed model/header/phase pointer.
pub const DerivedWeight = struct {
    kind: enum { input_projection, gate_up },
    sources: [4]u64,
    widths: [4]usize,
    hidden: usize,
    pub fn count(self: DerivedWeight) usize {
        return if (self.kind == .gate_up) 2 else 4;
    }
    pub fn bytes(self: DerivedWeight) !usize {
        if (self.hidden == 0) return error.InvalidPreparedCacheAmount;
        var width: usize = 0;
        for (0..self.count()) |i| {
            if (self.sources[i] == 0 or self.widths[i] == 0) return error.InvalidPreparedCacheAmount;
            width = try std.math.add(usize, width, self.widths[i]);
        }
        for (self.count()..4) |i| if (self.sources[i] != 0 or self.widths[i] != 0) return error.InvalidPreparedCacheAmount;
        return std.math.mul(usize, try std.math.mul(usize, width, self.hidden), 2);
    }
};
pub const Named = struct { name: Name, bytes: usize };
/// Private operation workspace with independent retained host/device backing.
/// Shape and policy are certified by its concrete owner before any SDK action.
pub const Auxiliary = struct {
    pub const Kind = enum { rotary_positions_u32, rounded_bf16_output, pinned_metadata_host, pinned_input_host, burst_readback_host };
    pub const count = std.meta.fieldNames(Kind).len;
    kind: Kind,
    device_bytes: usize,
    host_bytes: usize = 0,
    pub fn validate(self: Auxiliary) !void {
        if (self.device_bytes == 0 and self.kind != .pinned_metadata_host and self.kind != .pinned_input_host and self.kind != .burst_readback_host) return error.InvalidPreparedCacheAmount;
        switch (self.kind) {
            .rotary_positions_u32 => if (self.host_bytes != self.device_bytes or self.device_bytes % @sizeOf(u32) != 0) return error.InvalidPreparedCacheAmount,
            .rounded_bf16_output => if (self.host_bytes != 0 or self.device_bytes % @sizeOf(u16) != 0) return error.InvalidPreparedCacheAmount,
            .pinned_metadata_host, .pinned_input_host, .burst_readback_host => if (self.device_bytes != 0 or self.host_bytes == 0) return error.InvalidPreparedCacheAmount,
        }
    }
    pub fn amounts(self: Auxiliary) memory.AdmissionAmounts {
        return .{ .backend_scratch_bytes = self.device_bytes, .host_scratch_bytes = self.host_bytes };
    }
};
/// Exact value-only retained capture planes. The concrete graph owner creates
/// these from the SAME StorageLayout used by reset; the key hash distinguishes
/// one synchronous funding phase, never replaces full-key owner validation.
pub const CapturedStorage = struct {
    epoch: u64,
    key_hash: u64,
    key_words: usize,
    rows: usize,
    hidden: usize,
    vocab: usize,
    metadata_bytes: usize,
    host_bytes: usize,
    head_bytes: usize,
    pub fn amounts(self: CapturedStorage) !memory.AdmissionAmounts {
        return .{ .backend_scratch_bytes = try std.math.add(usize, self.metadata_bytes, self.head_bytes), .host_scratch_bytes = self.host_bytes };
    }
};
pub const CapturedArena = struct {
    epoch: u64,
    key_hash: u64,
    bytes: usize,
    events: usize,
    pub fn amounts(self: CapturedArena) memory.AdmissionAmounts {
        return .{ .backend_scratch_bytes = self.bytes };
    }
};

pub const Request = union(enum) {
    derived_weight: DerivedWeight,
    captured_storage: CapturedStorage,
    captured_arena: CapturedArena,
    auxiliary: Auxiliary,
    resize: Resize,
    source: struct { index: u1, bytes: usize },
    arena: Arena,
    packed_weight: Named,
    constant: Named,
    decoder: @import("video_cache_budget.zig").Sequence,
    /// Independently charged idle device-cache/deferred-free envelope.
    ocr_retention: struct { cache_bytes: usize, deferred_bytes: usize, pressure_step: u8 },
    /// Original OCR retention pressure geometry, independent of SDK/model
    /// state. Callers advance steps only after an actual cold supplier denial.
    pub fn retentionEnvelope(configured_cache: usize, configured_deferred: usize, limits: memory.Limits, pressure_step: usize) Request {
        var cache = configured_cache;
        var deferred = configured_deferred;
        var retained_limit: usize = std.math.maxInt(usize);
        for ([_]usize{ limits.backend_limit_bytes, limits.combined_limit_bytes, limits.scratch_limit_bytes }) |limit| {
            if (limit != 0) retained_limit = @min(retained_limit, limit / 2);
        }
        while (cache > std.math.maxInt(usize) - deferred or cache + deferred > retained_limit) {
            cache /= 2;
            deferred /= 2;
        }
        var step: usize = 0;
        while (step < @min(pressure_step, @bitSizeOf(usize))) : (step += 1) {
            cache /= 2;
            deferred /= 2;
        }
        return .{ .ocr_retention = .{ .cache_bytes = cache, .deferred_bytes = deferred, .pressure_step = @intCast(@min(pressure_step, @bitSizeOf(usize))) } };
    }
    pub fn packedSpan(name: []const u8, bytes: usize) !Request {
        return .{ .packed_weight = .{ .name = try Name.init(name), .bytes = bytes } };
    }
    pub fn constantSpan(name: []const u8, bytes: usize) !Request {
        return .{ .constant = .{ .name = try Name.init(name), .bytes = bytes } };
    }
    pub fn joinedPackedSpan(names: []const []const u8, widths: []const usize, in_dim: usize) !Request {
        if (names.len < 2 or names.len > 3 or names.len != widths.len or in_dim == 0) return error.InvalidPreparedCacheName;
        var width: usize = 0;
        for (widths) |value| width = try std.math.add(usize, width, value);
        var text: [256]u8 = undefined;
        const prefix = try std.fmt.bufPrint(&text, "audio-packed-f32-output:{d}:{d}:", .{ in_dim, width });
        var used = prefix.len;
        for (names, 0..) |name, index| {
            if (name.len == 0 or name.len > text.len - used) return error.InvalidPreparedCacheName;
            if (index != 0) {
                if (used == text.len) return error.InvalidPreparedCacheName;
                text[used] = '|';
                used += 1;
            }
            if (name.len > text.len - used) return error.InvalidPreparedCacheName;
            @memcpy(text[used..][0..name.len], name);
            used += name.len;
        }
        return packedSpan(text[0..used], try std.math.mul(usize, try std.math.mul(usize, width, in_dim), 2));
    }
};
pub const Workload = struct { requests: []const Request };

/// Caller-owned synchronous metadata. Equal requests coalesce; different spans
/// for the same semantic cache key are rejected instead of funding ambiguity.
pub const Builder = struct {
    storage: []Request,
    len: usize = 0,
    pub fn add(self: *Builder, request: Request) !void {
        for (self.storage[0..self.len]) |prior| {
            if (std.meta.eql(prior, request)) return;
            const conflict = switch (request) {
                .packed_weight => |value| switch (prior) {
                    .packed_weight => |old| std.mem.eql(u8, old.name.slice(), value.name.slice()),
                    else => false,
                },
                .ocr_retention => prior == .ocr_retention,
                .captured_storage => prior == .captured_storage,
                .captured_arena => prior == .captured_arena,
                .auxiliary => |value| switch (prior) {
                    .auxiliary => |old| old.kind == value.kind,
                    else => false,
                },
                .constant => |value| switch (prior) {
                    .constant => |old| std.mem.eql(u8, old.name.slice(), value.name.slice()),
                    else => false,
                },
                else => false,
            };
            if (conflict) return error.ConflictingPreparedCacheRequest;
        }
        if (self.len == self.storage.len) return error.PreparedCacheRequestCapacity;
        self.storage[self.len] = request;
        self.len += 1;
    }
    pub fn workload(self: *const Builder) Workload {
        return .{ .requests = self.storage[0..self.len] };
    }
};

pub const Funding = struct {
    allocator: std.mem.Allocator,
    entries: []Entry,
    heap_owned: bool = true,
    deny_cold_misses: bool = false,
    optional_disabled: bool = false,
    decoder_sequence: ?@import("video_cache_budget.zig").Sequence = null,
    deny_constants: bool = false,
    compute_identity: usize = 0,
    admission: ?policy = null,
    domain_pin: ?domain.Pin = null,
    boundary: ?*const sink.Epoch = null, // ONLY during actual Stage-owned gate
    state: enum { created, snapped, funded, bound, unbound, retired } = .created,
    pub const Entry = struct {
        request: Request,
        miss: bool = false,
        amounts: memory.AdmissionAmounts = .{},
        temporary_host_bytes: usize = 0,
        optional_declined: bool = false,
        upgrade_raw: bool = false,
        upgrade: retirement.PreparedCharge = .{ .owner = null },
        lease: ?memory.AdmissionLease = null,
        temporary: ?memory.AdmissionLease = null,
        failure: retirement.PreparedCharge = .{ .owner = null },
        state: enum { unavailable, ready, taken, published, failed } = .unavailable,
    };
    pub fn empty() Funding {
        return .{ .allocator = undefined, .entries = &.{}, .heap_owned = false, .state = .funded };
    }
    /// The run's original compatibility envelope remains admitted. An optional
    /// cache reservation decline preserves that route without a late supplier.
    pub fn compatibility() Funding {
        var value = empty();
        value.deny_cold_misses = true;
        return value;
    }
    /// Ordinary warm misses are discovered without allocating Funding. This
    /// constructor is cold-only and called BEFORE model/provider ownership.
    pub fn create(work: Workload) !*Funding {
        return createWithAllocator(platform.allocator.processAllocator(platform.allocator.concurrentFallback()), work);
    }
    /// Cold ownership injection. The allocator must outlive all deferred
    /// funding metadata; production callers use a stable process allocator.
    pub fn createWithAllocator(a: std.mem.Allocator, work: Workload) !*Funding {
        if (work.requests.len == 0) return error.EmptyPreparedCacheWorkload;
        const self = try a.create(Funding);
        errdefer a.destroy(self);
        const entries = try a.alloc(Entry, work.requests.len);
        errdefer a.free(entries);
        for (entries, work.requests) |*entry, request| entry.* = .{ .request = request };
        // Duplicate keys would let one phase spend the same semantic slot twice.
        for (entries, 0..) |entry, i| for (entries[0..i]) |prior| {
            if (std.meta.eql(entry.request, prior.request)) return error.DuplicatePreparedCacheRequest;
        };
        self.* = .{ .allocator = a, .entries = entries };
        return self;
    }
    /// Fill only from exact canonical cache-owner exclusion. No SDK/supplier or
    /// allocation here. The backend supplies actual retained + build peak bytes.
    pub fn noteMiss(self: *Funding, index: usize, amounts: memory.AdmissionAmounts, temporary_host: usize) !void {
        if (self.state != .created or index >= self.entries.len) return error.InvalidPreparedCacheState;
        if (std.meta.eql(amounts, memory.AdmissionAmounts{})) return error.InvalidPreparedCacheAmount;
        self.entries[index].miss = true;
        self.entries[index].amounts = amounts;
        self.entries[index].temporary_host_bytes = temporary_host;
    }
    /// One atomic full-overlap reservation before ANY payload allocation. All
    /// external-token partition headers and failure shells prepare outside gates.
    pub fn fundOutsideExecution(self: *Funding, pin: ?*const domain.Pin) !void {
        if (self.state != .snapped) return error.InvalidPreparedCacheState;
        const actual = self.admission orelse {
            for (self.entries) |*entry| if (entry.miss) {
                entry.state = .ready;
            };
            self.state = .funded;
            return;
        };
        const owned = pin orelse return error.MissingCacheRetirementDomain;
        if (owned.controller() != actual.controller) return error.ForeignAdmissionDomain;
        var total: memory.AdmissionAmounts = .{};
        for (self.entries) |entry| if (entry.miss) {
            total = try total.merge(entry.amounts);
            total = try total.merge(.{ .host_scratch_bytes = entry.temporary_host_bytes });
        };
        if (std.meta.eql(total, memory.AdmissionAmounts{})) {
            self.state = .funded;
            return;
        }
        self.domain_pin = owned.retain();
        errdefer {
            self.domain_pin.?.release();
            self.domain_pin = null;
        }
        var source: ?memory.AdmissionLease = try actual.controller.tryAcquire(.gpu, actual.limits, total, actual.check_live_memory);
        defer if (source) |*lease| lease.release();
        // Every error leaves complete ownership in this cold object; its
        // outside destructor returns all children, with no SDK state published.
        errdefer self.releaseFundedOutside();
        for (self.entries) |*entry| if (entry.miss) {
            entry.failure = try retirement.PreparedCharge.prepare(0);
            var prepared = try source.?.preparePartition(self.allocator, entry.amounts);
            entry.lease = prepared.commit();
            if (entry.temporary_host_bytes != 0) {
                var scratch = try source.?.preparePartition(self.allocator, .{ .host_scratch_bytes = entry.temporary_host_bytes });
                entry.temporary = scratch.commit();
            }
            entry.state = .ready;
        };
        self.state = .funded;
    }
    /// A decoder is mandatory. If the combined optional reservation declines,
    /// the actual outside-lane owner may retry ONLY the exact mandatory set.
    /// A second denial is the original public decoder pressure failure.
    pub fn retainOnlyRequiredOutsideExecution(self: *Funding) !bool {
        if (self.state != .snapped) return error.InvalidPreparedCacheState;
        var required = false;
        for (self.entries) |*entry| {
            if (entry.lease != null or entry.temporary != null or entry.failure.owner != null) return error.InvalidPreparedCacheState;
            if (entry.request == .decoder) required = required or entry.miss else entry.miss = false;
        }
        self.optional_disabled = true;
        return required;
    }
    /// OCR preserves the historical priority: idle/deferred retention is
    /// admitted before optional adjusted norm weights. Resource denial of norm
    /// banks never halves an already accepted retention envelope. Every SDK
    /// action still waits for actual owner revalidation and binding.
    pub fn fundLightOnOutsideExecution(self: *Funding, pin: *const domain.Pin) !void {
        if (self.state != .snapped) return error.InvalidPreparedCacheState;
        const actual = self.admission orelse return error.MissingCacheRetirementDomain;
        if (pin.controller() != actual.controller) return error.ForeignAdmissionDomain;
        var retention: memory.AdmissionAmounts = .{};
        var constants: memory.AdmissionAmounts = .{};
        var retention_count: usize = 0;
        for (self.entries) |entry| switch (entry.request) {
            .ocr_retention => {
                retention_count += 1;
                if (entry.miss) retention = try retention.merge(entry.amounts);
            },
            .constant => if (entry.miss) {
                constants = try constants.merge(entry.amounts);
            },
            .derived_weight => |value| {
                if (entry.miss and !std.meta.eql(entry.amounts, memory.AdmissionAmounts{ .backend_weight_bytes = try value.bytes() })) return error.InvalidPreparedCacheAmount;
            },
            .auxiliary => |value| {
                try value.validate();
                if (entry.miss and !std.meta.eql(entry.amounts, value.amounts())) return error.InvalidPreparedCacheAmount;
            },
            else => return error.UnsupportedPreparedCacheWorkload,
        };
        if (retention_count != 1) return error.InvalidPreparedCacheState;
        self.domain_pin = pin.retain();
        errdefer {
            self.releaseFundedOutside();
            self.domain_pin.?.release();
            self.domain_pin = null;
        }
        var retention_source: ?memory.AdmissionLease = null;
        defer if (retention_source) |*lease| lease.release();
        if (!std.meta.eql(retention, memory.AdmissionAmounts{})) retention_source = try actual.controller.tryAcquire(.gpu, actual.limits, retention, actual.check_live_memory);
        var constant_source: ?memory.AdmissionLease = null;
        defer if (constant_source) |*lease| lease.release();
        if (!std.meta.eql(constants, memory.AdmissionAmounts{})) {
            constant_source = actual.controller.tryAcquire(.gpu, actual.limits, constants, actual.check_live_memory) catch |err| switch (err) {
                error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable => null,
            };
            self.deny_constants = constant_source == null;
        }
        for (self.entries) |*entry| if (entry.miss and entry.request != .auxiliary and entry.request != .derived_weight) {
            if (entry.request == .constant and self.deny_constants) continue;
            const source = if (entry.request == .ocr_retention) &retention_source else &constant_source;
            if (entry.temporary_host_bytes != 0) return error.InvalidPreparedCacheAmount;
            entry.failure = try retirement.PreparedCharge.prepare(0);
            var prepared = try source.*.?.preparePartition(self.allocator, entry.amounts);
            entry.lease = prepared.commit();
            entry.state = .ready;
        };
        for (self.entries) |*entry| if (entry.upgrade_raw) {
            if (entry.request != .auxiliary and entry.request != .derived_weight) return error.InvalidPreparedCacheState;
            entry.upgrade = try retirement.PreparedCharge.prepare(0);
        };
        // Preserve already accepted retention and norm banks. Each optional
        // workspace has its own original reservation and can decline alone.
        for (self.entries) |*entry| if (entry.miss and (entry.request == .auxiliary or entry.request == .derived_weight)) {
            if (entry.temporary_host_bytes != 0) return error.InvalidPreparedCacheAmount;
            entry.failure = try retirement.PreparedCharge.prepare(0);
            entry.lease = actual.controller.tryAcquire(.gpu, actual.limits, entry.amounts, actual.check_live_memory) catch |err| switch (err) {
                error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable => null,
            };
            if (entry.lease == null) entry.optional_declined = true else entry.state = .ready;
        };
        self.state = .funded;
    }
    /// Invoked only after actual backend identity/revision/policy revalidation.
    /// The Stage retains this exact epoch; no creation token is stored later.
    pub fn bind(self: *Funding, epoch: *const sink.Epoch) !void {
        // Rebinding after a real pause is metadata-only. The concrete backend
        // MUST first revalidate exact live owners/policy; spent entries never
        // become ready again and cannot manufacture another cache admission.
        if ((self.state != .funded and self.state != .unbound) or try epoch.isSealed()) return error.InvalidPreparedCacheState;
        self.boundary = epoch;
        self.state = .bound;
    }
    /// Clear the Context's borrowed binding BEFORE this and before model unlock.
    /// Unconsumed quotas retire only in finishOutsideExecution afterwards.
    pub fn unbind(self: *Funding) void {
        std.debug.assert(self.state == .bound);
        for (self.entries) |entry| std.debug.assert(entry.state != .taken);
        self.boundary = null;
        self.decoder_sequence = null;
        self.state = .unbound;
    }
    pub fn take(self: *Funding, request: Request, actual: memory.AdmissionAmounts) !Lease {
        if (self.state != .bound or (request == .constant and self.deny_constants)) return error.UnpreparedCacheAdmission;
        for (self.entries) |*entry| if (entry.miss and std.meta.eql(entry.request, request)) {
            if (entry.optional_declined) return error.UnpreparedCacheAdmission;
            if (!std.meta.eql(entry.amounts, actual)) return error.PreparedCacheAmountChanged;
            if (entry.state != .ready) return error.PreparedCacheAdmissionAlreadyConsumed;
            entry.state = .taken;
            const value = entry.lease;
            entry.lease = null;
            return .{ .value = value, .owner = self, .entry = entry };
        };
        return error.UnpreparedCacheAdmission;
    }
    /// A copy can fail after the newly allocated packed bank was published in
    /// its cache map. The SAME current phase owns its prepared rollback shell;
    /// no Entry pointer is ever stored in that persistent map.
    pub fn rollbackPublished(self: *Funding, request: Request, value: *CacheQuota) void {
        if (self.state != .bound) platform.inference_process_supervisor.restartWorker();
        for (self.entries) |*entry| if (entry.miss and std.meta.eql(entry.request, request)) {
            if (entry.state != .published) platform.inference_process_supervisor.restartWorker();
            const epoch = self.boundary orelse platform.inference_process_supervisor.restartWorker();
            value.releaseAfterSdkDeath(.{ .boundary = epoch }) catch platform.inference_process_supervisor.restartWorker();
            entry.state = .failed;
            return;
        };
        platform.inference_process_supervisor.restartWorker();
    }
    fn releaseFundedOutside(self: *Funding) void {
        for (self.entries) |*entry| {
            if (entry.lease) |*lease| lease.release();
            entry.lease = null;
            if (entry.temporary) |*lease| lease.release();
            entry.temporary = null;
            entry.failure.destroyUnused();
            entry.upgrade.destroyUnused();
        }
    }
    pub fn finishOutsideExecution(self: *Funding) void {
        std.debug.assert(self.state != .bound);
        for (self.entries) |entry| std.debug.assert(entry.state != .taken);
        self.releaseFundedOutside();
        if (self.domain_pin) |pin| pin.release();
        self.domain_pin = null;
        if (!self.heap_owned) {
            self.state = .retired;
            return;
        }
        const a = self.allocator;
        a.free(self.entries);
        a.destroy(self);
    }
};

/// A cached payload receives only its exact independently owned lease. A cold
/// failure moves it into the preallocated pure Charge AFTER checked SDK death.
pub const Lease = struct {
    value: ?memory.AdmissionLease = null,
    owner: ?*Funding = null,
    entry: ?*Funding.Entry = null,
    pub fn funded(self: Lease) bool {
        return self.owner != null;
    }
    /// Successful caches retain the SAME cold shell for their future actual
    /// final death, instead of discarding it when this invocation ends.
    pub fn publishQuota(self: *Lease) CacheQuota {
        const owner = self.owner orelse return CacheQuota.fromRawAdmission(self.publish());
        const entry = self.entry.?;
        std.debug.assert(entry.state == .taken);
        if (self.value == null) {
            _ = self.publish();
            return .none;
        }
        var pin: ?domain.Pin = owner.domain_pin.?.retain();
        const quota = CacheQuota.arm(&entry.failure, &self.value, &pin) catch platform.inference_process_supervisor.restartWorker();
        entry.state = .published;
        self.owner = null;
        self.entry = null;
        return quota;
    }
    pub fn publish(self: *Lease) ?memory.AdmissionLease {
        const value = self.value;
        self.value = null;
        if (self.entry) |entry| entry.state = .published;
        self.owner = null;
        self.entry = null;
        return value;
    }
    /// No SDK/payload publication has been attempted. Return the exact token
    /// to its phase for ordinary outside-lane cleanup, without any callback or
    /// registration against a possibly-invalid construction boundary.
    pub fn cancelBeforeSdk(self: *Lease) !void {
        const owner = self.owner orelse return error.UnpreparedCacheAdmission;
        const entry = self.entry orelse return error.InvalidPreparedCacheState;
        if (owner.state != .bound or entry.state != .taken or entry.lease != null) return error.InvalidPreparedCacheState;
        entry.lease = self.value;
        entry.state = .ready;
        self.* = .{};
    }
    /// The cached opaque decoder owns its future error-retirement metadata.
    /// No phase entry or construction epoch escapes into that persistent owner.
    pub fn publishDecoder(self: *Lease, budget: *@import("video_cache_budget.zig").CacheBudget, physical: *@import("allocation_budget.zig").Budget, bytes: usize) !void {
        if (budget.bytes != 0 or budget.quota.hasLease()) return error.InvalidVideoCacheReservation;
        const owner = self.owner orelse return error.UnpreparedCacheAdmission;
        const entry = self.entry.?;
        if (owner.state != .bound or entry.request != .decoder or try entry.request.decoder.bytes() != bytes) return error.PreparedCacheAmountChanged;
        const current = owner.boundary orelse return error.InvalidPreparedCacheState;
        if (try current.isSealed()) return error.RetirementEpochSealed;
        try physical.reserve(bytes);
        budget.physical = physical;
        budget.bytes = bytes;
        budget.quota = self.publishQuota(); // SAME common prepared shell/domain
    }
    pub fn releaseAfterSdkDeath(self: *Lease) void {
        if (self.owner) |owner| {
            const entry = self.entry.?;
            const epoch = owner.boundary orelse platform.inference_process_supervisor.restartWorker();
            const boundary = epoch.boundary() catch platform.inference_process_supervisor.restartWorker();
            if (self.value != null) {
                const pin = owner.domain_pin.?.retain();
                var optional_pin: ?domain.Pin = pin;
                const charge = entry.failure.arm(&self.value, &optional_pin) catch platform.inference_process_supervisor.restartWorker();
                charge.holdUntilBoundary(boundary) catch platform.inference_process_supervisor.restartWorker();
                charge.release();
            }
            entry.state = .failed;
        } else if (self.value) |*lease| lease.release();
        self.value = null;
        self.owner = null;
        self.entry = null;
    }
};

/// Existing cache producers share this single typed admission seam. Only an
/// explicit Stage binding permits the allocation-free prefunded branch.
pub fn acquire(ctx: anytype, request: Request, amounts: memory.AdmissionAmounts) !Lease {
    if (ctx.prepared_cache_funding) |funding| return funding.take(request, amounts);
    return acquireRaw(ctx, amounts);
}

fn coldFundingOomCase(a: std.mem.Allocator) !void {
    const owned_domain = try domain.Domain.create();
    defer owned_domain.release();
    const pin = owned_domain.pin();
    defer pin.release();
    const External = struct {
        active: usize = 0,
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.active += 1;
            return 1;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.active == 1);
            self.active = 0;
        }
    };
    var external: External = .{};
    try pin.controller().configureResourceBudget(.{ .context = &external, .try_reserve = External.reserve, .retain = External.retain, .release = External.release });
    defer std.testing.expectEqual(@as(usize, 0), external.active) catch @panic("cold partition failure leaked source external token");
    const requests = [_]Request{ try Request.packedSpan("packed.fixture", 16), try Request.constantSpan("audio_tower.prepared.fixture", 32) };
    const funding = try Funding.createWithAllocator(a, .{ .requests = &requests });
    defer funding.finishOutsideExecution();
    try funding.noteMiss(0, .{ .backend_scratch_bytes = 16 }, 8);
    try funding.noteMiss(1, .{ .backend_scratch_bytes = 32 }, 0);
    funding.admission = .{ .controller = pin.controller(), .limits = .{ .backend_limit_bytes = 48, .host_limit_bytes = 8 }, .check_live_memory = false };
    funding.state = .snapped;
    funding.fundOutsideExecution(&pin) catch |err| {
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, pin.controller().snapshot());
        return err;
    };
    try std.testing.expectEqual(@as(usize, 48), pin.controller().snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 8), pin.controller().snapshot().host_scratch_bytes);
}

test "CUDA prepared cache funding cold metadata all OOM and cap denial publish no quota or SDK state" {
    try coldFundingOomCase(std.testing.allocator);
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, coldFundingOomCase, .{});
    const owned_domain = try domain.Domain.create();
    defer owned_domain.release();
    const pin = owned_domain.pin();
    defer pin.release();
    const requests = [_]Request{try Request.packedSpan("packed.fixture", 16)};
    const funding = try Funding.create(.{ .requests = &requests });
    defer funding.finishOutsideExecution();
    try funding.noteMiss(0, .{ .backend_scratch_bytes = 16 }, 0);
    funding.admission = .{ .controller = pin.controller(), .limits = .{ .backend_limit_bytes = 15 }, .check_live_memory = false };
    funding.state = .snapped;
    try std.testing.expectError(error.ResourceLimitExceeded, funding.fundOutsideExecution(&pin));
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, pin.controller().snapshot());
}

test "CUDA prepared cache funding moves published tokens and retires failed construction only after actual current B" {
    const Session = @import("../../backends/session.zig").Session;
    const ModelOwner = struct {
        fn close(_: *anyopaque) void {}
    };
    var model_marker: u8 = 0;
    var model: Session.ModelLifetime = .{ .owner = &model_marker, .destroy_fn = ModelOwner.close };
    const Probe = struct {
        model: *Session.ModelLifetime,
        reserve_calls: usize = 0,
        callback_under_gate: bool = false,
        next: usize = 1,
        fn check(self: *@This()) void {
            if (!self.model.execution_gate.tryLock()) self.callback_under_gate = true else self.model.execution_gate.unlock();
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            self.reserve_calls += 1;
            const next = self.next;
            self.next += 1;
            return next;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
        }
        fn retainContext(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            return true;
        }
        fn releaseContext(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
        }
    };
    const owned_domain = try domain.Domain.create();
    defer owned_domain.release();
    const pin = owned_domain.pin();
    defer pin.release();
    var probe: Probe = .{ .model = &model };
    try pin.controller().configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release, .retain_context = Probe.retainContext, .release_context = Probe.releaseContext });
    var requests = [_]Request{ try Request.packedSpan("packed.fixture", 16), try Request.constantSpan("audio_tower.prepared.fixture", 32) };
    const funding = try Funding.create(.{ .requests = &requests });
    var funding_open = true;
    defer if (funding_open) funding.finishOutsideExecution();
    try funding.noteMiss(0, .{ .backend_scratch_bytes = 16 }, 8);
    try funding.noteMiss(1, .{ .backend_scratch_bytes = 32 }, 0);
    funding.admission = .{ .controller = pin.controller(), .limits = .{}, .check_live_memory = false };
    funding.state = .snapped;
    try funding.fundOutsideExecution(&pin);
    try std.testing.expectEqual(@as(usize, 1), probe.reserve_calls); // atomic full overlap
    var owner = try sink.Ref.create();
    defer owner.release();
    var guard = try owner.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try std.testing.expect(model.execution_gate.tryLock());
    var locked = true;
    defer if (locked) {
        if (funding.state == .bound) funding.unbind();
        model.execution_gate.unlock();
    };
    try funding.bind(&epoch);
    var published = try funding.take(requests[0], .{ .backend_scratch_bytes = 16 });
    var cache_lease = published.publishQuota();
    defer if (cache_lease.hasLease()) {
        if (locked) {
            if (funding.state == .bound) funding.unbind();
            model.execution_gate.unlock();
            locked = false;
        }
        cache_lease.releaseOutsideExecution();
    };
    var failed = try funding.take(requests[1], .{ .backend_scratch_bytes = 32 });
    defer if (failed.owner != null) failed.releaseAfterSdkDeath();
    try std.testing.expectError(error.PreparedCacheAdmissionAlreadyConsumed, funding.take(requests[1], .{ .backend_scratch_bytes = 32 }));
    failed.releaseAfterSdkDeath(); // Pure construction-failure seam; no GPU claim.
    try std.testing.expectEqual(@as(usize, 48), pin.controller().snapshot().backend_scratch_bytes);
    funding.unbind();
    model.execution_gate.unlock();
    locked = false;
    // A caller's work/name stack can disappear; all token/late callback metadata
    // and actual cache lease are independent owned values.
    requests[1] = try Request.constantSpan("destroyed.caller.name", 1);
    funding.finishOutsideExecution();
    funding_open = false;
    try std.testing.expectEqual(@as(usize, 0), pin.controller().snapshot().host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 48), pin.controller().snapshot().backend_scratch_bytes);
    guard.finish();
    try std.testing.expectEqual(@as(usize, 16), pin.controller().snapshot().backend_scratch_bytes);
    // Publication A is already sealed; this actual final cache death belongs
    // to independently current B, never the original construction boundary.
    try std.testing.expectError(error.RetirementEpochSealed, cache_lease.validateCleanup(.{ .boundary = &epoch }));
    try std.testing.expectEqual(@as(usize, 16), pin.controller().snapshot().backend_scratch_bytes);
    var final_guard = try owner.enter();
    defer final_guard.finish();
    var final_epoch = try final_guard.retainEpoch();
    defer final_epoch.release();
    try std.testing.expect(model.execution_gate.tryLock());
    locked = true;
    try cache_lease.releaseAfterSdkDeath(.{ .boundary = &final_epoch });
    try std.testing.expectEqual(@as(usize, 16), pin.controller().snapshot().backend_scratch_bytes);
    model.execution_gate.unlock();
    locked = false;
    final_guard.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, pin.controller().snapshot());
    try std.testing.expect(!probe.callback_under_gate);
}

/// Byte-only compatibility adapter for the preexisting raw cache API. Real
/// Stage bindings always use the checked Request overload above.
pub fn acquireRaw(ctx: anytype, amounts: memory.AdmissionAmounts) !Lease {
    std.debug.assert(ctx.prepared_cache_funding == null);
    const admission = ctx.video_cache_budget.admission orelse return .{};
    return .{ .value = try admission.controller.tryAcquire(.gpu, admission.limits, amounts, admission.check_live_memory) };
}

test "CUDA prepared cache builder copies exact keys coalesces repeated requests and refuses conflicting spans" {
    var storage: [2]Request = undefined;
    var builder: Builder = .{ .storage = &storage };
    var name = [_]u8{ 'q', 'k', 'v' };
    try builder.add(try Request.packedSpan(&name, 96));
    name[0] = 'X';
    try builder.add(try Request.packedSpan("qkv", 96));
    try std.testing.expectEqual(@as(usize, 1), builder.workload().requests.len);
    try std.testing.expectError(error.ConflictingPreparedCacheRequest, builder.add(try Request.packedSpan("qkv", 128)));
    try builder.add(try Request.constantSpan("audio_tower.prepared.fixture", 16));
    try std.testing.expectError(error.PreparedCacheRequestCapacity, builder.add(try Request.packedSpan("other", 16)));
    try std.testing.expectEqual(@as(usize, 2), builder.workload().requests.len);
    const joined = try Request.joinedPackedSpan(&.{ "query", "key", "value" }, &.{ 4, 2, 2 }, 3);
    try std.testing.expectEqualStrings("audio-packed-f32-output:3:8:query|key|value", joined.packed_weight.name.slice());
    try std.testing.expectEqual(@as(usize, 48), joined.packed_weight.bytes);
}

test "CUDA mandatory decoder funding keeps opaque envelope and transfers future current B shell without construction epoch" {
    const Video = @import("video_cache_budget.zig");
    const Physical = @import("allocation_budget.zig").Budget;
    const Session = @import("../../backends/session.zig").Session;
    const ModelOwner = struct {
        fn close(_: *anyopaque) void {}
    };
    var model_marker: u8 = 0;
    var model: Session.ModelLifetime = .{ .owner = &model_marker, .destroy_fn = ModelOwner.close };
    const Probe = struct {
        model: *Session.ModelLifetime,
        under_gate: bool = false,
        next: usize = 1,
        fn check(self: *@This()) void {
            if (!self.model.execution_gate.tryLock()) self.under_gate = true else self.model.execution_gate.unlock();
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            const id = self.next;
            self.next += 1;
            return id;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
        }
    };
    const owned_domain = try domain.Domain.create();
    defer owned_domain.release();
    const pin = owned_domain.pin();
    defer pin.release();
    var probe: Probe = .{ .model = &model };
    try pin.controller().configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    const sequence: Video.Sequence = .{ .width = 160, .height = 96, .coded_width = 160, .coded_height = 96, .surfaces = 4, .codec = 4, .display_left = 0, .display_top = 0, .matrix = 1, .full_range = 0 };
    const bytes = try sequence.bytes();
    const requests = [_]Request{ .{ .decoder = sequence }, try Request.packedSpan("optional.fixture", 16) };
    const funding = try Funding.create(.{ .requests = &requests });
    var funding_open = true;
    defer if (funding_open) funding.finishOutsideExecution();
    try funding.noteMiss(0, .{ .backend_scratch_bytes = bytes }, 0);
    try funding.noteMiss(1, .{ .backend_scratch_bytes = 16 }, 0);
    funding.admission = .{ .controller = pin.controller(), .limits = .{ .backend_limit_bytes = bytes }, .check_live_memory = false };
    funding.state = .snapped;
    try std.testing.expectError(error.ResourceLimitExceeded, funding.fundOutsideExecution(&pin));
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, pin.controller().snapshot());
    try std.testing.expect(try funding.retainOnlyRequiredOutsideExecution());
    try funding.fundOutsideExecution(&pin);
    var owner = try sink.Ref.create();
    defer owner.release();
    var first = try owner.enter();
    defer first.finish();
    var a = try first.retainEpoch();
    defer a.release();
    try funding.bind(&a);
    defer if (funding_open and funding.state == .bound) funding.unbind();
    try std.testing.expectError(error.UnpreparedCacheAdmission, funding.take(requests[1], .{ .backend_scratch_bytes = 16 }));
    var token = try funding.take(requests[0], .{ .backend_scratch_bytes = bytes });
    defer if (token.owner != null) token.releaseAfterSdkDeath();
    var physical: Physical = .{ .limit = bytes };
    var budget: Video.CacheBudget = .{};
    defer budget.release(); // explicit outside-execution test teardown
    try token.publishDecoder(&budget, &physical, bytes);
    funding.unbind();
    funding.finishOutsideExecution();
    funding_open = false;
    first.finish();
    a.release();
    // Cache keeps only pure domain + inert shell; original epoch is releasable
    // and reusable, not cached as authority for a later decoder failure.
    for (0..65) |_| {
        var later = try owner.enter();
        later.finish();
    }
    var final = try owner.enter();
    defer final.finish();
    var b = try final.retainEpoch();
    defer b.release();
    try std.testing.expect(model.execution_gate.tryLock());
    var locked = true;
    defer if (locked) model.execution_gate.unlock();
    try budget.releaseAfterSdkDeath(&b); // pure SDK-death seam, not a decoder claim
    try std.testing.expectEqual(@as(usize, 0), physical.snapshot().live);
    try std.testing.expectEqual(bytes, pin.controller().snapshot().backend_scratch_bytes);
    model.execution_gate.unlock();
    locked = false;
    final.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, pin.controller().snapshot());
    try std.testing.expect(!probe.under_gate);
}

test "CUDA LightOn cold cache funding preserves page then retention then optional norm priority" {
    const owner = try domain.Domain.create();
    defer owner.release();
    const pin = owner.pin();
    defer pin.release();
    const limits: memory.Limits = .{ .backend_limit_bytes = 32 };
    // The actual caller must already own PAGE admission; this primitive never
    // guesses page headroom from a policy limit or promises a later supplier.
    var page = try pin.controller().tryAcquire(.gpu, limits, .{ .backend_scratch_bytes = 8 }, false);
    defer page.release();
    const requests = [_]Request{
        .{ .ocr_retention = .{ .cache_bytes = 12, .deferred_bytes = 4, .pressure_step = 0 } },
        try Request.constantSpan("lighton.norm_weight.fixture", 16),
    };
    const funding = try Funding.create(.{ .requests = &requests });
    defer funding.finishOutsideExecution();
    try funding.noteMiss(0, .{ .backend_scratch_bytes = 16 }, 0);
    try funding.noteMiss(1, .{ .backend_scratch_bytes = 16 }, 0);
    funding.admission = .{ .controller = pin.controller(), .limits = limits, .check_live_memory = false };
    funding.state = .snapped;
    try funding.fundLightOnOutsideExecution(&pin);
    try std.testing.expect(funding.deny_constants);
    try std.testing.expectEqual(@as(usize, 24), pin.controller().snapshot().backend_scratch_bytes);
    try std.testing.expect(funding.entries[0].state == .ready);
    try std.testing.expect(funding.entries[1].state == .unavailable);
    // No numerical root is needed to test strict metadata ownership. Actual
    // CUDA SDK-death coverage remains in the retained-cache hardware fixtures.
    var source = try sink.Ref.create();
    defer source.release();
    var guard = try source.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try funding.bind(&epoch);
    var lease = try funding.take(requests[0], .{ .backend_scratch_bytes = 16 });
    var quota = lease.publishQuota();
    defer quota.releaseOutsideExecution();
    try std.testing.expectError(error.UnpreparedCacheAdmission, funding.take(requests[1], .{ .backend_scratch_bytes = 16 }));
    funding.unbind();
    try quota.releaseAfterSdkDeath(.{ .boundary = &epoch });
    try std.testing.expectEqual(@as(usize, 24), pin.controller().snapshot().backend_scratch_bytes);
    guard.finish();
    try std.testing.expectEqual(@as(usize, 8), pin.controller().snapshot().backend_scratch_bytes);
}

test "CUDA LightOn retention geometry preserves explicit ceilings overflow and finite pressure exhaustion" {
    const preferred = Request.retentionEnvelope(96, 32, .{ .backend_limit_bytes = 200 }, 0).ocr_retention;
    try std.testing.expectEqual(@as(usize, 48), preferred.cache_bytes);
    try std.testing.expectEqual(@as(usize, 16), preferred.deferred_bytes);
    const tighter = Request.retentionEnvelope(96, 32, .{ .backend_limit_bytes = 200, .combined_limit_bytes = 100, .scratch_limit_bytes = 60 }, 0).ocr_retention;
    try std.testing.expectEqual(@as(usize, 12), tighter.cache_bytes);
    try std.testing.expectEqual(@as(usize, 4), tighter.deferred_bytes);
    const pressure = Request.retentionEnvelope(96, 32, .{ .backend_limit_bytes = 200 }, 1).ocr_retention;
    try std.testing.expectEqual(@as(usize, 24), pressure.cache_bytes);
    try std.testing.expectEqual(@as(usize, 8), pressure.deferred_bytes);
    const overflow = Request.retentionEnvelope(std.math.maxInt(usize), std.math.maxInt(usize), .{}, 0).ocr_retention;
    try std.testing.expectEqual(std.math.maxInt(usize) / 2, overflow.cache_bytes);
    try std.testing.expectEqual(std.math.maxInt(usize) / 2, overflow.deferred_bytes);
    const exhausted = Request.retentionEnvelope(std.math.maxInt(usize), std.math.maxInt(usize), .{}, @bitSizeOf(usize)).ocr_retention;
    try std.testing.expectEqual(@as(usize, 0), exhausted.cache_bytes);
    try std.testing.expectEqual(@as(usize, 0), exhausted.deferred_bytes);
}

fn lightOnFundingOomCase(a: std.mem.Allocator) !void {
    const owned_domain = try domain.Domain.create();
    defer owned_domain.release();
    const pin = owned_domain.pin();
    defer pin.release();
    const External = struct {
        active: usize = 0,
        next: usize = 0,
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.active += 1;
            self.next += 1;
            return self.next;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.active != 0);
            self.active -= 1;
        }
    };
    var external: External = .{};
    try pin.controller().configureResourceBudget(.{ .context = &external, .try_reserve = External.reserve, .retain = External.retain, .release = External.release });
    defer std.testing.expectEqual(@as(usize, 0), external.active) catch @panic("OCR cold partition failure leaked supplier token");
    const requests = [_]Request{
        .{ .ocr_retention = .{ .cache_bytes = 12, .deferred_bytes = 4, .pressure_step = 0 } },
        try Request.constantSpan("lighton.norm_weight.fixture", 16),
    };
    const funding = try Funding.createWithAllocator(a, .{ .requests = &requests });
    defer funding.finishOutsideExecution();
    try funding.noteMiss(0, .{ .backend_scratch_bytes = 16 }, 0);
    try funding.noteMiss(1, .{ .backend_scratch_bytes = 16 }, 0);
    funding.admission = .{ .controller = pin.controller(), .limits = .{ .backend_limit_bytes = 32 }, .check_live_memory = false };
    funding.state = .snapped;
    funding.fundLightOnOutsideExecution(&pin) catch |err| {
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, pin.controller().snapshot());
        return err;
    };
    try std.testing.expectEqual(@as(usize, 32), pin.controller().snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 2), external.active);
    try std.testing.expect(!funding.deny_constants);
}

test "CUDA LightOn two priority cold reservations unwind all metadata OOM and original supplier tokens" {
    try lightOnFundingOomCase(std.testing.allocator);
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, lightOnFundingOomCase, .{});
}

test "CUDA auxiliary cache priority retains page norm and independent workspace amounts on optional denial" {
    const actual = try domain.Domain.create();
    defer actual.release();
    const pin = actual.pin();
    defer pin.release();
    const limits = memory.Limits{ .backend_limit_bytes = 32, .host_limit_bytes = 32, .combined_limit_bytes = 48 };
    var page = try actual.controller.tryAcquire(.gpu, limits, .{ .backend_scratch_bytes = 8 }, false);
    defer page.release();
    const requests = [_]Request{
        Request.retentionEnvelope(8, 0, limits, 0),
        try Request.constantSpan("quota-only.norm.fixture", 4),
        .{ .auxiliary = .{ .kind = .rotary_positions_u32, .device_bytes = 32, .host_bytes = 32 } },
        .{ .auxiliary = .{ .kind = .rounded_bf16_output, .device_bytes = 4 } },
    };
    // This tests the resource owner only; actual cache recipes are separately
    // certified by CudaCompute before any payload allocation or dispatch.
    const funding = try Funding.createWithAllocator(std.testing.allocator, .{ .requests = &requests });
    defer funding.finishOutsideExecution();
    try funding.noteMiss(0, .{ .backend_scratch_bytes = 8 }, 0);
    try funding.noteMiss(1, .{ .backend_scratch_bytes = 4 }, 0);
    try funding.noteMiss(2, requests[2].auxiliary.amounts(), 0);
    try funding.noteMiss(3, requests[3].auxiliary.amounts(), 0);
    funding.admission = .{ .controller = &actual.controller, .limits = limits, .check_live_memory = false };
    funding.state = .snapped;
    try funding.fundLightOnOutsideExecution(&pin);
    try std.testing.expect(!funding.deny_constants);
    try std.testing.expect(funding.entries[0].state == .ready);
    try std.testing.expect(funding.entries[1].state == .ready);
    try std.testing.expect(funding.entries[2].optional_declined);
    try std.testing.expect(funding.entries[3].state == .ready);
    try std.testing.expectEqual(@as(usize, 24), actual.controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), actual.controller.snapshot().host_scratch_bytes);
    try std.testing.expectError(error.InvalidPreparedCacheAmount, (Auxiliary{ .kind = .rotary_positions_u32, .device_bytes = 4, .host_bytes = 3 }).validate());
    try std.testing.expectError(error.InvalidPreparedCacheAmount, (Auxiliary{ .kind = .rounded_bf16_output, .device_bytes = 3 }).validate());
}

test "CUDA mandatory decoder rejects sealed publication before physical reservation and restores unused token" {
    const Video = @import("video_cache_budget.zig");
    const Physical = @import("allocation_budget.zig").Budget;
    const owner = try domain.Domain.create();
    defer owner.release();
    const pin = owner.pin();
    defer pin.release();
    const sequence: Video.Sequence = .{ .width = 160, .height = 96, .coded_width = 160, .coded_height = 96, .surfaces = 4, .codec = 4, .display_left = 0, .display_top = 0, .matrix = 1, .full_range = 0 };
    const amount = try sequence.bytes();
    const request: Request = .{ .decoder = sequence };
    const funding = try Funding.create(.{ .requests = &.{request} });
    defer funding.finishOutsideExecution();
    try funding.noteMiss(0, .{ .backend_scratch_bytes = amount }, 0);
    funding.admission = .{ .controller = pin.controller(), .limits = .{}, .check_live_memory = false };
    funding.state = .snapped;
    try funding.fundOutsideExecution(&pin);
    var source = try sink.Ref.create();
    defer source.release();
    var guard = try source.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try funding.bind(&epoch);
    defer if (funding.state == .bound) funding.unbind();
    var token = try funding.take(request, .{ .backend_scratch_bytes = amount });
    defer if (token.owner != null) token.cancelBeforeSdk() catch @panic("unused decoder token restoration failed");
    guard.finish(); // Invalid caller publication is refused BEFORE any SDK.
    var physical: Physical = .{};
    var budget: Video.CacheBudget = .{};
    try std.testing.expectError(error.RetirementEpochSealed, token.publishDecoder(&budget, &physical, amount));
    try std.testing.expectEqual(@as(usize, 0), physical.snapshot().live);
    try std.testing.expectEqual(@as(usize, 0), budget.bytes);
    try std.testing.expect(!budget.quota.hasLease());
    try std.testing.expectEqual(amount, pin.controller().snapshot().backend_scratch_bytes);
    try token.cancelBeforeSdk();
    try std.testing.expect(funding.entries[0].state == .ready);
    funding.unbind();
}

test "CUDA derived weight cold funding uses weight class preserves scratch floors and retires at current B" {
    const owner = try domain.Domain.create();
    defer owner.release();
    const pin = owner.pin();
    defer pin.release();
    const request: Request = .{ .derived_weight = .{ .kind = .input_projection, .sources = .{ 1, 2, 3, 4 }, .widths = .{ 6144, 2048, 16, 16 }, .hidden = 1024 } };
    const bytes = try request.derived_weight.bytes();
    try std.testing.expectEqual(@as(usize, 16842752), bytes);
    const requests = [_]Request{ .{ .ocr_retention = .{ .cache_bytes = 8, .deferred_bytes = 8, .pressure_step = 0 } }, request };
    var floor = try pin.controller().tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 32 }, false);
    defer floor.release();
    for ([_]usize{ bytes - 1, bytes }) |weight_limit| {
        const funding = try Funding.create(.{ .requests = &requests });
        defer funding.finishOutsideExecution();
        try funding.noteMiss(0, .{ .backend_scratch_bytes = 16 }, 0);
        try funding.noteMiss(1, .{ .backend_weight_bytes = bytes }, 0);
        funding.admission = .{ .controller = pin.controller(), .limits = .{ .backend_limit_bytes = weight_limit + 48, .scratch_limit_bytes = 48 }, .check_live_memory = false };
        funding.state = .snapped;
        try funding.fundLightOnOutsideExecution(&pin);
        try std.testing.expectEqual(@as(usize, 48), pin.controller().snapshot().backend_scratch_bytes);
        try std.testing.expectEqual(weight_limit < bytes, funding.entries[1].optional_declined);
        var source = try sink.Ref.create();
        defer source.release();
        var guard = try source.enter();
        defer guard.finish();
        var epoch = try guard.retainEpoch();
        defer epoch.release();
        try funding.bind(&epoch);
        if (weight_limit < bytes) {
            try std.testing.expectError(error.UnpreparedCacheAdmission, funding.take(request, .{ .backend_weight_bytes = bytes }));
        } else {
            var lease = try funding.take(request, .{ .backend_weight_bytes = bytes });
            var quota = lease.publishQuota();
            try quota.releaseAfterSdkDeath(.{ .boundary = &epoch });
            try std.testing.expectEqual(bytes, pin.controller().snapshot().backend_weight_bytes);
        }
        funding.unbind();
        guard.finish();
        try std.testing.expectEqual(@as(usize, 0), pin.controller().snapshot().backend_weight_bytes);
    }
    var invalid = request.derived_weight;
    invalid.widths[0] = std.math.maxInt(usize);
    try std.testing.expectError(error.Overflow, invalid.bytes());
    invalid = .{ .kind = .gate_up, .sources = .{ 1, 2, 3, 0 }, .widths = .{ 3584, 3584, 0, 0 }, .hidden = 1024 };
    try std.testing.expectError(error.InvalidPreparedCacheAmount, invalid.bytes());
}

fn derivedWeightFundingOomCase(a: std.mem.Allocator) !void {
    const owned_domain = try domain.Domain.create();
    defer owned_domain.release();
    const pin = owned_domain.pin();
    defer pin.release();
    const External = struct {
        active: usize = 0,
        next: usize = 0,
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.active += 1;
            self.next += 1;
            return self.next;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.active != 0);
            self.active -= 1;
        }
    };
    var external: External = .{};
    try pin.controller().configureResourceBudget(.{ .context = &external, .try_reserve = External.reserve, .retain = External.retain, .release = External.release });
    defer std.testing.expectEqual(@as(usize, 0), external.active) catch @panic("derived weight cold failure leaked supplier token");
    const requests = [_]Request{
        .{ .ocr_retention = .{ .cache_bytes = 12, .deferred_bytes = 4, .pressure_step = 0 } },
        .{ .derived_weight = .{ .kind = .gate_up, .sources = .{ 1, 2, 0, 0 }, .widths = .{ 4, 4, 0, 0 }, .hidden = 1 } },
    };
    const funding = try Funding.createWithAllocator(a, .{ .requests = &requests });
    defer funding.finishOutsideExecution();
    try funding.noteMiss(0, .{ .backend_scratch_bytes = 16 }, 0);
    try funding.noteMiss(1, .{ .backend_weight_bytes = 16 }, 0);
    funding.admission = .{ .controller = pin.controller(), .limits = .{ .backend_limit_bytes = 32 }, .check_live_memory = false };
    funding.state = .snapped;
    funding.fundLightOnOutsideExecution(&pin) catch |err| {
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, pin.controller().snapshot());
        return err;
    };
    try std.testing.expectEqual(@as(usize, 16), pin.controller().snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 16), pin.controller().snapshot().backend_weight_bytes);
    try std.testing.expectEqual(@as(usize, 2), external.active);
    try std.testing.expect(!funding.deny_constants);
}

test "CUDA derived weight cold reservation unwinds all metadata OOM and exact original tokens" {
    try derivedWeightFundingOomCase(std.testing.allocator);
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, derivedWeightFundingOomCase, .{});
}
