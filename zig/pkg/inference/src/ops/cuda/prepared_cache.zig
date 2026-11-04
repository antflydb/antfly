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
pub const Named = struct { name: Name, bytes: usize };
pub const Request = union(enum) {
    resize: Resize,
    source: struct { index: u1, bytes: usize },
    arena: Arena,
    packed_weight: Named,
    constant: Named,
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
    fn createWithAllocator(a: std.mem.Allocator, work: Workload) !*Funding {
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
    /// Invoked only after actual backend identity/revision/policy revalidation.
    /// The Stage retains this exact epoch; no creation token is stored later.
    pub fn bind(self: *Funding, epoch: *const sink.Epoch) !void {
        if (self.state != .funded or try epoch.isSealed()) return error.InvalidPreparedCacheState;
        self.boundary = epoch;
        self.state = .bound;
    }
    /// Clear the Context's borrowed binding BEFORE this and before model unlock.
    /// Unconsumed quotas retire only in finishOutsideExecution afterwards.
    pub fn unbind(self: *Funding) void {
        std.debug.assert(self.state == .bound);
        for (self.entries) |entry| std.debug.assert(entry.state != .taken);
        self.boundary = null;
        self.state = .unbound;
    }
    pub fn take(self: *Funding, request: Request, actual: memory.AdmissionAmounts) !Lease {
        if (self.state != .bound) return error.UnpreparedCacheAdmission;
        for (self.entries) |*entry| if (entry.miss and std.meta.eql(entry.request, request)) {
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
    pub fn rollbackPublished(self: *Funding, request: Request, value: *?memory.AdmissionLease) void {
        if (self.state != .bound) platform.inference_process_supervisor.restartWorker();
        for (self.entries) |*entry| if (entry.miss and std.meta.eql(entry.request, request)) {
            if (entry.state != .published) platform.inference_process_supervisor.restartWorker();
            entry.state = .taken;
            var lease: Lease = .{ .owner = self, .entry = entry, .value = value.* };
            value.* = null;
            lease.releaseAfterSdkDeath();
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
    pub fn publish(self: *Lease) ?memory.AdmissionLease {
        const value = self.value;
        self.value = null;
        if (self.entry) |entry| entry.state = .published;
        self.owner = null;
        self.entry = null;
        return value;
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
    var cache_lease = published.publish();
    defer if (cache_lease) |*lease| {
        if (locked) {
            if (funding.state == .bound) funding.unbind();
            model.execution_gate.unlock();
            locked = false;
        }
        lease.release();
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
    cache_lease.?.release();
    cache_lease = null;
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
