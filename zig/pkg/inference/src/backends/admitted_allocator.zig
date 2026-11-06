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

//! Request-local allocation owner with chunked admission and complete rollback.
//! Small allocations reuse capacity without resource-manager/IPC calls.
const std = @import("std");
const sessions = @import("session.zig");
const memory = @import("../runtime/tier/memory.zig");

pub const AdmittedAllocator = struct {
    backing: std.mem.Allocator,
    session: ?sessions.Session = null,
    admission_context: ?*anyopaque = null,
    admit: ?*const fn (*anyopaque, usize) anyerror!memory.AdmissionLease = null,
    trim_on_free: bool = false,
    live_bytes: usize = 0,
    reserved_bytes: usize = 0,
    /// Borrowed from a live execution permit. Its owner must outlive every
    /// allocation and must not reduce the permit while this credit is bound.
    prepaid_host_bytes: usize = 0,
    /// Pure failure guard set by the actual Source numerical owner. It can
    /// only refuse growth/refund; it grants no allocation or cleanup authority.
    numerical_scope_active: bool = false,
    metadata_bytes: usize = 0,
    admission_error: ?anyerror = null,
    blocks: ?*Header = null,
    reservations: ?*Reservation = null,

    const quantum = 64 * 1024;
    const Header = struct { prev: ?*Header, next: ?*Header, bytes: usize, alignment: std.mem.Alignment };
    const Reservation = struct { next: ?*Reservation, lease: ?memory.AdmissionLease, bytes: usize };
    pub const reservation_overhead = @sizeOf(Reservation);

    /// Exact additional physical host bytes for one constructor allocation
    /// through this allocator and its existing prepaid RunScope node. This is
    /// disjoint from every original host/device/output requirement.
    pub fn constructionHostBytes(bytes: usize, alignment: std.mem.Alignment) !usize {
        return try std.math.add(usize, try allocationHostBytes(bytes, alignment), @sizeOf(Reservation));
    }

    /// Physical payload plus this allocator's real tracking header only.
    /// RunScope/Reservation metadata is accounted once by its own owner.
    pub fn allocationHostBytes(bytes: usize, alignment: std.mem.Alignment) !usize {
        return std.math.add(usize, bytes, allocationOverhead(alignment)) catch error.ResourceLimitExceeded;
    }

    /// Declare exact future host allocations without acquiring or lending
    /// quota. The existing Source cold path remains the sole admission owner.
    /// A bound allocator's actual trim funds its current live bytes separately;
    /// this Source must fund every future producer and its one RunScope node.
    pub fn declareRunHostAllocations(session: sessions.Session, request: *sessions.RunRequest, future_bytes: usize, allocator_bound: bool) !void {
        const admission = session.run_admission orelse return;
        const required = std.math.add(usize, future_bytes, if (allocator_bound) @sizeOf(Reservation) else 0) catch return error.ResourceLimitExceeded;
        const original = try admission.planRequest(request.*, session.outputInfo());
        const extra = required -| original.protected.host_scratch_bytes;
        request.host_preprocess_bytes = std.math.add(usize, request.host_preprocess_bytes, extra) catch return error.ResourceLimitExceeded;
    }

    const allocator_vtable: std.mem.Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };

    pub fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &allocator_vtable };
    }

    /// The exact transparent allocation source, while this admitted owner is
    /// live. Unknown/copied allocator tables grant no ancestry proof.
    pub fn backingAllocator(value: std.mem.Allocator) ?std.mem.Allocator {
        if (value.vtable != &allocator_vtable) return null;
        const self: *@This() = @ptrCast(@alignCast(value.ptr));
        return self.backing;
    }

    pub fn allocationOverhead(alignment: std.mem.Alignment) usize {
        return std.mem.alignForward(usize, @sizeOf(Header), alignment.toByteUnits());
    }

    fn reserve(self: *@This(), amount: usize) !?memory.AdmissionLease {
        if (self.admit) |callback| return try callback(self.admission_context.?, amount);
        const session = self.session orelse return error.ResourceLimitExceeded;
        // Separate chunk leases still belong to one request. Its own retained
        // bytes cannot turn an intrinsically oversized request into contention.
        if (session.run_admission) |admission| {
            const owned = std.math.add(usize, self.reserved_bytes, self.prepaid_host_bytes) catch return error.ResourceLimitExceeded;
            const total = std.math.add(usize, owned, amount) catch return error.ResourceLimitExceeded;
            try admission.validateOwnedHostScratch(total);
        }
        return (try session.admitHostPreprocess(amount)).lease;
    }

    /// Execution borrows this permit's workspace. On exit, adopt its lease
    /// before returning unused capacity, so surviving outputs never lose their
    /// admission and no fresh reservation is required during cleanup.
    pub const RunScope = struct {
        permit: sessions.RunPermit,
        owner: ?*AdmittedAllocator = null,
        reservation: ?*Reservation = null,
        /// Prepared only at the final-address Source before gates. The original
        /// complete lease stays intact until all numerical SDK payloads die.
        host_retirement: ?memory.AdmissionLease.PreparedPartition = null,

        /// Cold original-run output partition. The remaining prepaid capacity
        /// must still cover every actual source allocation and metadata byte;
        /// no supplier or allocation is reached for an unfunded source.
        pub fn prepareHostOutputsBeforeExecution(self: *@This(), pin: *const @import("../runtime/tier/admission_domain.zig").Pin, count: usize, width: usize) !sessions.HostOutputProducer {
            if (self.owner) |owner| if (owner.numerical_scope_active) return error.InvalidAdmissionLeaseReduction;
            const lease = if (self.permit.lease) |*value| value else return error.MissingHostOutputEscrow;
            const bytes = try sessions.HostOutputProducer.requiredBytes(count, width);
            if (self.owner) |owner| {
                const capacity = try std.math.add(usize, owner.reserved_bytes, owner.prepaid_host_bytes);
                const used = try std.math.add(usize, owner.live_bytes, owner.metadata_bytes);
                if (bytes > owner.prepaid_host_bytes or used > capacity - bytes) return error.HostOutputEscrowWouldUnfundSource;
            }
            const producer = try sessions.HostOutputProducer.prepare(lease, pin, count, width);
            // Partition commitment was the last fallible step. Never lend the
            // transferred output bytes again to Native source allocations.
            if (self.owner) |owner| owner.prepaid_host_bytes -= bytes;
            self.refreshHostRetirementAfterPartition();
            return producer;
        }

        /// Cold transfer of an ADDITIVE bank declared by this exact original
        /// run. The original source floor and every already-live allocation
        /// remain funded; only committed child bytes leave prepaid capacity.
        pub fn prepareDeclaredTaskHostOutputBeforeExecution(self: *@This()) !sessions.HostOutputProducer {
            if (self.owner) |owner| if (owner.numerical_scope_active) return error.InvalidAdmissionLeaseReduction;
            const bytes = try self.permit.declaredHostOutputBytes();
            if (self.owner) |owner| {
                const capacity = try std.math.add(usize, owner.reserved_bytes, owner.prepaid_host_bytes);
                const used = try std.math.add(usize, owner.live_bytes, owner.metadata_bytes);
                if (bytes > owner.prepaid_host_bytes or used > capacity - bytes) return error.HostOutputEscrowWouldUnfundSource;
            }
            const producer = try self.permit.prepareDeclaredHostOutputOutsideExecution();
            if (self.owner) |owner| owner.prepaid_host_bytes -= bytes;
            self.refreshHostRetirementAfterPartition();
            return producer;
        }

        /// Called before the actual Source acquires numerical Model/lane.
        /// Outside source preparation may still grow through original policy.
        pub fn beginNumerical(self: *@This()) !void {
            if (self.owner) |owner| {
                if (owner.numerical_scope_active) return error.InvalidAdmissionLeaseReduction;
                // GPU scopes prepare this at final-address cold init, including
                // every no-enter/cancellation path. Native pure-host needs none.
                owner.numerical_scope_active = true;
            }
        }
        fn prepareHostRetirement(self: *@This()) !void {
            const owner = self.owner orelse return;
            if (self.host_retirement != null) return error.InvalidAdmissionLeaseReduction;
            if (self.permit.lease) |*lease| {
                const host: memory.AdmissionAmounts = .{ .host_scratch_bytes = owner.prepaid_host_bytes };
                if (!std.meta.eql(lease.amounts, host))
                    self.host_retirement = try lease.preparePartition(owner.backing, host);
            }
        }
        fn refreshHostRetirementAfterPartition(self: *@This()) void {
            // The just-committed source bank either published the shared token
            // or the original source has no external token. Re-preparing its
            // remaining host subset cannot allocate/call a supplier. This fresh
            // exclusively-owned source has at most task/per-source/host members.
            if (self.host_retirement) |*prepared| prepared.abort();
            self.host_retirement = null;
            if (self.owner != null and self.permit.lease != null) {
                std.debug.assert(self.permit.lease.?.external_lease == null);
                self.prepareHostRetirement() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            }
        }
        fn discardFailedColdInitialization(self: *@This()) void {
            if (self.host_retirement) |*prepared| prepared.abort();
            self.host_retirement = null;
            if (self.owner) |owner| {
                owner.prepaid_host_bytes = 0;
                owner.metadata_bytes -= @sizeOf(Reservation);
                owner.backing.destroy(self.reservation.?);
            }
            self.owner = null;
            self.reservation = null;
            self.permit.deinit();
        }
        /// Caller has released its actual Model and lane. No callback here.
        pub fn endNumerical(self: *@This()) void {
            if (self.owner) |owner| owner.numerical_scope_active = false;
        }

        /// Pure typed-final teardown only. No borrowed payload may remain;
        /// the caller moves this exact permit lease into its cold current-B
        /// Charge after numerical retirement. No trim/supplier callback here.
        pub fn validateEmptyPrepaidRetirement(self: *const @This()) !void {
            if (self.owner) |owner| {
                const actual = owner.session orelse return error.InvalidAdmissionLeaseReduction;
                if (actual.ptr != self.permit.session.ptr or actual.vtable != self.permit.session.vtable or actual.model_lifetime != self.permit.session.model_lifetime) return error.InvalidAdmissionLeaseReduction;
                if (owner.live_bytes != 0) return error.SourceForeignWorkspaceStillLive;
                if (self.reservation == null or owner.prepaid_host_bytes == 0) return error.InvalidAdmissionLeaseReduction;
            }
        }
        pub fn detachEmptyPrepaidFunding(self: *@This()) !void {
            try self.validateEmptyPrepaidRetirement();
            // Foreign teardown moves the complete original lease into its
            // existing current-B charge; no host payload survives this path.
            if (self.host_retirement) |*prepared| prepared.abort();
            self.host_retirement = null;
            if (self.owner) |owner| {
                owner.prepaid_host_bytes = 0;
                owner.metadata_bytes -= @sizeOf(Reservation);
                owner.backing.destroy(self.reservation.?);
                self.reservation = null;
                self.owner = null;
            }
        }

        pub fn deinit(self: *@This()) void {
            self.deinitWithCleanup(.{});
        }

        /// Caller has released its actual model/provider lanes. Preserve its
        /// live whole-consumer boundary while the SAME workspace lease moves.
        pub fn deinitWithCleanup(self: *@This(), current: @import("../execution/cleanup_context.zig").Context) void {
            if (self.owner) |owner| {
                std.debug.assert(!owner.numerical_scope_active);
                const node = self.reservation.?;
                // SDK death and owner unlock already completed. Commit the
                // cold prepared class split before transferring CPU custody;
                // zero host spare or a refused host shrink can never retain
                // the original device floor in a host Reservation.
                const host_lease = if (self.host_retirement) |*prepared| prepared.commit() else self.permit.lease;
                const separated = self.host_retirement != null;
                self.host_retirement = null;
                node.* = .{ .next = owner.reservations, .lease = host_lease, .bytes = owner.prepaid_host_bytes };
                owner.reservations = node;
                owner.reserved_bytes += owner.prepaid_host_bytes;
                owner.prepaid_host_bytes = 0;
                if (!separated) self.permit.lease = null;
                // A failed external reduction conservatively keeps its lease.
                // Later phases retry trim; final teardown always releases it.
                owner.trim() catch {};
            }
            self.permit.deinitWithCleanup(current);
            self.owner = null;
        }
    };

    pub fn admitRun(self: *@This(), request: sessions.RunRequest) !RunScope {
        return self.admitRunWithTaskHostOutput(request, null);
    }
    pub fn admitRunWithTaskHostOutput(self: *@This(), request: sessions.RunRequest, task_output: ?sessions.RunPermit.HostOutputGeometry) !RunScope {
        // Returning Native scopes preserve the original API. A mixed GPU scope
        // must first reach its final address for its cold prepared class split.
        if (self.session) |session| if (session.run_admission) |admission| {
            if (admission.backend_class != .cpu) return error.InvalidAdmissionLeaseReduction;
        };
        return self.admitRunWithTaskHostOutputUnpublished(request, task_output);
    }
    pub fn admitRunInto(self: *@This(), result: *RunScope, request: sessions.RunRequest) !void {
        return self.admitRunWithTaskHostOutputInto(result, request, null);
    }
    pub fn admitRunWithTaskHostOutputInto(self: *@This(), result: *RunScope, request: sessions.RunRequest, task_output: ?sessions.RunPermit.HostOutputGeometry) !void {
        result.* = try self.admitRunWithTaskHostOutputUnpublished(request, task_output);
        errdefer result.discardFailedColdInitialization();
        try result.prepareHostRetirement();
    }
    fn admitRunWithTaskHostOutputUnpublished(self: *@This(), request: sessions.RunRequest, task_output: ?sessions.RunPermit.HostOutputGeometry) !RunScope {
        const session = self.session orelse return error.InvalidAdmissionLeaseReduction;
        const admission = session.run_admission orelse {
            if (task_output != null) return error.UnmanagedHostOutput;
            return .{ .permit = try session.admit(request) };
        };
        if (self.admit != null or self.prepaid_host_bytes != 0) return error.InvalidAdmissionLeaseReduction;
        // Prepay ONLY the same permit's host_scratch class, for CPU or GPU
        // numerical backends. Backend scratch/KV/weights never become allocator
        // capacity; the original six-class source floor remains independently
        // present in the SAME execution lease.
        var amounts = try admission.estimateRequest(request, session.outputInfo());
        if (amounts.host_kv_bytes != 0) return error.InvalidAdmissionLeaseReduction;
        if (amounts.host_scratch_bytes < @sizeOf(Reservation)) return .{ .permit = try admitTaskRequest(session, request, task_output) };
        if (task_output) |output| amounts.host_scratch_bytes = try std.math.add(usize, amounts.host_scratch_bytes, try output.freshBytes());
        try self.trim();
        // Use the ACTUAL post-trim owner, not a caller's preview or old chunk
        // slack. Mandatory producers belong to this SAME six-class request;
        // intrinsic combined denial must precede any controller acquisition.
        var owned = amounts;
        owned.host_scratch_bytes = std.math.add(usize, owned.host_scratch_bytes, self.reserved_bytes) catch return error.ResourceLimitExceeded;
        try admission.validateOwnedRunAmounts(owned);
        if (task_output) |output| try output.validateHeld(session, owned);
        var permit = try admitTaskRequest(session, request, task_output);
        errdefer permit.deinit();
        // The execution lease covers this node too. Allocate it now so cleanup
        // can transfer retained outputs without allocation or reacquisition.
        const node = try self.backing.create(Reservation);
        self.metadata_bytes += @sizeOf(Reservation);
        self.prepaid_host_bytes = amounts.host_scratch_bytes;
        return .{ .permit = permit, .owner = self, .reservation = node };
    }

    fn admitTaskRequest(session: sessions.Session, request: sessions.RunRequest, task_output: ?sessions.RunPermit.HostOutputGeometry) !sessions.RunPermit {
        if (task_output) |output| {
            var pressure: ?memory.AdmissionPressure = null;
            return session.admitWithTaskHostOutputWithPressure(request, output, &pressure);
        }
        return session.admit(request);
    }

    pub fn ensureCapacity(self: *@This(), extra: usize) !void {
        const needed = try std.math.add(usize, try std.math.add(usize, self.live_bytes, self.metadata_bytes), extra);
        const capacity = try std.math.add(usize, self.reserved_bytes, self.prepaid_host_bytes);
        if (needed <= capacity) return;
        // A live RunScope already owns its declared host demand. Never invoke
        // an external supplier while its actual Model/lane may be held, nor
        // borrow unrelated device workspace to hide missing host metadata.
        if (self.numerical_scope_active) return error.ResourceLimitExceeded;
        const minimum = try std.math.add(usize, needed - capacity, @sizeOf(Reservation));
        var amount = @max(minimum, quantum);
        var lease = self.reserve(amount) catch |err| blk: {
            if (amount == minimum or (err != error.ResourceLimitExceeded and err != error.ResourceTemporarilyUnavailable)) return err;
            amount = minimum; // Slack must not reject requests fitting tight limits.
            break :blk try self.reserve(amount);
        };
        errdefer if (lease) |*active| active.release();
        const reservation = try self.backing.create(Reservation);
        reservation.* = .{ .next = self.reservations, .lease = lease, .bytes = amount };
        self.reservations = reservation;
        self.metadata_bytes += @sizeOf(Reservation);
        self.reserved_bytes += amount;
    }

    pub fn trim(self: *@This()) !void {
        // Runtime frees may request eager trimming. Keep all pure reservation
        // callbacks for the original RunScope's post-unlock teardown.
        if (self.numerical_scope_active) return error.ResourceTemporarilyUnavailable;
        // A reservation must not keep its own bookkeeping allocation alive
        // after the last payload is freed. Release both owners together.
        if (self.live_bytes == 0) {
            while (self.reservations) |reservation| {
                self.reservations = reservation.next;
                if (reservation.lease) |*lease| lease.release();
                self.reserved_bytes -= reservation.bytes;
                self.metadata_bytes -= @sizeOf(Reservation);
                self.backing.destroy(reservation);
            }
            return;
        }
        var link = &self.reservations;
        while (link.*) |reservation| {
            const spare = (self.reserved_bytes + self.prepaid_host_bytes) -| (self.live_bytes + self.metadata_bytes);
            const reduction = @min(spare, reservation.bytes);
            if (reduction != 0) {
                if (reservation.lease) |*lease| try lease.retain(.{ .host_scratch_bytes = reservation.bytes - reduction });
                reservation.bytes -= reduction;
                self.reserved_bytes -= reduction;
            }
            if (reservation.bytes == 0) {
                link.* = reservation.next;
                if (reservation.lease) |*lease| lease.release();
                self.metadata_bytes -= @sizeOf(Reservation);
                self.backing.destroy(reservation);
            } else link = &reservation.next;
        }
    }

    /// Includes allocations abandoned by a failing tokenizer.
    pub fn deinit(self: *@This()) void {
        std.debug.assert(!self.numerical_scope_active);
        while (self.blocks) |block| {
            self.blocks = block.next;
            self.backing.rawFree(@as([*]u8, @ptrCast(block))[0..block.bytes], block.alignment, @returnAddress());
        }
        while (self.reservations) |reservation| {
            self.reservations = reservation.next;
            var lease = reservation.lease;
            self.backing.destroy(reservation);
            if (lease) |*active| active.release();
        }
        self.live_bytes = 0;
        self.reserved_bytes = 0;
        self.metadata_bytes = 0;
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.admission_error = null;
        const bytes = std.math.add(usize, allocationOverhead(alignment), len) catch return null;
        self.ensureCapacity(bytes) catch |err| {
            self.admission_error = err;
            return null;
        };
        const base = self.backing.rawAlloc(bytes, alignment.max(.of(Header)), ra) orelse return null;
        const header: *Header = @ptrCast(@alignCast(base));
        header.* = .{ .prev = null, .next = self.blocks, .bytes = bytes, .alignment = alignment.max(.of(Header)) };
        if (self.blocks) |old| old.prev = header;
        self.blocks = header;
        self.live_bytes += bytes;
        return base + allocationOverhead(alignment);
    }

    // Logical shrink must not hide retained backing bytes from an outer
    // BoundedAllocator. A failed physical resize uses allocate/copy/free.
    fn resize(raw: *anyopaque, buffer: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const overhead = allocationOverhead(alignment);
        const base = buffer.ptr - overhead;
        const header: *Header = @ptrCast(@alignCast(base));
        if (new_len == buffer.len) return true;
        if (new_len == 0 or new_len > buffer.len) return false;
        const bytes = std.math.add(usize, overhead, new_len) catch return false;
        const old_bytes = header.bytes;
        if (!self.backing.rawResize(base[0..old_bytes], header.alignment, bytes, ra)) return false;
        header.bytes = bytes;
        self.live_bytes -= old_bytes - bytes;
        if (self.trim_on_free) self.trim() catch {};
        return true;
    }
    fn remap(raw: *anyopaque, buffer: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
        return if (resize(raw, buffer, alignment, new_len, ra)) buffer.ptr else null;
    }
    fn free(raw: *anyopaque, buffer: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const base = buffer.ptr - allocationOverhead(alignment);
        const header: *Header = @ptrCast(@alignCast(base));
        if (header.prev) |prev| prev.next = header.next else self.blocks = header.next;
        if (header.next) |next| next.prev = header.prev;
        self.live_bytes -= header.bytes;
        self.backing.rawFree(base[0..header.bytes], header.alignment, ra);
        if (self.trim_on_free and (self.live_bytes == 0 or (self.reserved_bytes + self.prepaid_host_bytes) -| (self.live_bytes + self.metadata_bytes) >= quantum)) self.trim() catch {};
    }
};

test "model aware media adaptive admission shares capacity grows and releases idle reservations" {
    const Provider = struct {
        controller: memory.AdmissionController = .{},
        fn acquire(raw: *anyopaque, bytes: usize) !memory.AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 160 * 1024 }, .{ .host_scratch_bytes = bytes }, false);
        }
    };
    var provider = Provider{};
    var first = AdmittedAllocator{ .backing = std.testing.allocator, .admission_context = &provider, .admit = Provider.acquire, .trim_on_free = true };
    defer first.deinit();
    var second = AdmittedAllocator{ .backing = std.testing.allocator, .admission_context = &provider, .admit = Provider.acquire, .trim_on_free = true };
    defer second.deinit();
    const one = try first.allocator().alloc(u8, 1024);
    const two = try second.allocator().alloc(u8, 1024);
    try std.testing.expect(provider.controller.snapshot().host_scratch_bytes < 160 * 1024);
    try std.testing.expectError(error.OutOfMemory, first.allocator().alloc(u8, 160 * 1024));
    try std.testing.expect(first.admission_error != null);
    first.allocator().free(one);
    try first.trim();
    second.allocator().free(two);
    try second.trim();
    // Repeated small requests must not accumulate zero-byte lease records.
    for (0..100) |_| {
        const bytes = try first.allocator().alloc(u8, 1024);
        first.allocator().free(bytes);
        try first.trim();
        try std.testing.expectEqual(@as(usize, 0), first.metadata_bytes);
        try std.testing.expectEqual(@as(usize, 0), first.reserved_bytes);
    }
    var no_resize = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    var resized = AdmittedAllocator{ .backing = no_resize.allocator(), .admission_context = &provider, .admit = Provider.acquire, .trim_on_free = true };
    defer resized.deinit();
    var ceiling = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = resized.allocator(), .limit = 2048 };
    var payload = try ceiling.allocator().alloc(u8, 1024);
    @memset(payload, 7);
    payload = try ceiling.allocator().realloc(payload, 512);
    try std.testing.expectEqual(@as(usize, 512), ceiling.live);
    try std.testing.expectEqual(ceiling.live + AdmittedAllocator.allocationOverhead(.of(u8)), resized.live_bytes);
    try std.testing.expectEqual(@as(u8, 7), payload[511]);
    ceiling.allocator().free(payload);
    try std.testing.expectEqual(@as(usize, 0), resized.reserved_bytes);
    first.deinit();
    second.deinit();
    try std.testing.expectEqual(@as(usize, 0), provider.controller.snapshot().host_scratch_bytes);
}

test "admission Gemma temporary ceiling does not reserve unused half gigabyte" {
    const MiB = 1024 * 1024;
    const limits = memory.Limits{ .host_limit_bytes = 2 * 1024 * MiB, .scratch_limit_bytes = 341 * MiB };
    var controller: memory.AdmissionController = .{};
    const session = sessions.Session{ .ptr = undefined, .vtable = undefined, .run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = limits,
        .static_workspace_bytes = 0,
        .check_live_memory = false,
    } };
    try std.testing.expectError(error.ResourceLimitExceeded, session.admitHostPreprocess(512 * MiB));
    var admitted = AdmittedAllocator{ .backing = std.testing.allocator, .session = session };
    defer admitted.deinit();
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = admitted.allocator(), .limit = 512 * MiB };
    const bytes = try bounded.allocator().alloc(u8, MiB);
    try std.testing.expect(controller.snapshot().host_scratch_bytes < 2 * MiB);
    var execution = try session.admitHostPreprocess(256 * MiB);
    execution.deinit();
    bounded.allocator().free(bytes);
    try admitted.trim();
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().hostTotalBytes());
    try std.testing.expectError(error.OutOfMemory, bounded.allocator().alloc(u8, 400 * MiB));
    try std.testing.expectEqual(@as(?anyerror, error.ResourceLimitExceeded), admitted.admission_error);
    var held = try session.admitHostPreprocess(320 * MiB);
    try std.testing.expectError(error.OutOfMemory, bounded.allocator().alloc(u8, 32 * MiB));
    try std.testing.expectEqual(@as(?anyerror, error.ResourceTemporarilyUnavailable), admitted.admission_error);
    held.deinit();
    const recovered = try bounded.allocator().alloc(u8, MiB);
    bounded.allocator().free(recovered);
    try admitted.trim();
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().hostTotalBytes());
}

test "admission native encoder allocations consume prepaid workspace and retain outputs across cohorts" {
    const MiB = 1024 * 1024;
    const Info = struct {
        fn outputs(_: *anyopaque) []const @import("tensor.zig").TensorInfo {
            return &.{};
        }
    };
    var vtable: sessions.Session.VTable = undefined;
    vtable.outputInfo = Info.outputs;
    var controller: memory.AdmissionController = .{};
    const session = sessions.Session{ .ptr = undefined, .vtable = &vtable, .run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{ .host_limit_bytes = 2048 * MiB, .scratch_limit_bytes = 341 * MiB },
        .static_workspace_bytes = 128 * MiB,
        .check_live_memory = false,
    } };
    var admitted = AdmittedAllocator{ .backing = std.testing.allocator, .session = session };
    defer admitted.deinit();
    const a = admitted.allocator();
    var prep: ?[]u8 = try a.alloc(u8, 64 * 1024);
    defer if (prep) |values| a.free(values);
    var outputs: [3]?[]f32 = @splat(null);
    defer for (outputs) |vector| if (vector) |values| a.free(values);
    for ([_]usize{ 135, 263, 391 }, 0..) |rows, index| {
        {
            var scope = try admitted.admitRun(try @import("../pipelines/embedding_gemma2_admission.zig").nativeTextEncoderRequest(4096, rows, 768));
            defer scope.deinit();
            const charged = controller.snapshot().host_scratch_bytes;
            const compute = try a.alloc(u8, 80 * MiB);
            defer a.free(compute);
            try std.testing.expectEqual(charged, controller.snapshot().host_scratch_bytes);
            outputs[index] = try a.alloc(f32, 768);
            @memset(outputs[index].?, @as(f32, @floatFromInt(index + 1)));
        }
        // Every earlier vector stays live, admitted and unchanged while the
        // next cohort reuses encoder workspace and returns its own output.
        try std.testing.expect(controller.snapshot().host_scratch_bytes >= admitted.live_bytes);
        try std.testing.expect(controller.snapshot().host_scratch_bytes < MiB);
        for (outputs[0 .. index + 1], 0..) |vector, prior| {
            for (vector.?) |value| try std.testing.expectEqual(@as(f32, @floatFromInt(prior + 1)), value);
        }
    }
    for (&outputs) |*vector| {
        a.free(vector.*.?);
        vector.* = null;
    }
    a.free(prep.?);
    prep = null;
    try admitted.trim();
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}

test "admission cumulative request scratch distinguishes intrinsic limits from other requests" {
    var controller: memory.AdmissionController = .{};
    const session = sessions.Session{ .ptr = undefined, .vtable = undefined, .run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{ .scratch_limit_bytes = 1024 * 1024 },
        .static_workspace_bytes = 0,
        .check_live_memory = false,
    } };
    var admitted = AdmittedAllocator{ .backing = std.testing.allocator, .session = session };
    defer admitted.deinit();
    const a = admitted.allocator();
    const first = try a.alloc(u8, 600 * 1024);
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 600 * 1024));
    try std.testing.expectEqual(@as(?anyerror, error.ResourceLimitExceeded), admitted.admission_error);
    a.free(first);
    try admitted.trim();
    var peer = try session.admitHostPreprocess(600 * 1024);
    defer peer.deinit();
    const small = try a.alloc(u8, 100 * 1024);
    defer a.free(small);
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 400 * 1024));
    try std.testing.expectEqual(@as(?anyerror, error.ResourceTemporarilyUnavailable), admitted.admission_error);
}

test "admission prepaid workspace grows only its deficit and rolls back failed scope construction" {
    const Info = struct {
        fn outputs(_: *anyopaque) []const @import("tensor.zig").TensorInfo {
            return &.{};
        }
    };
    var vtable: sessions.Session.VTable = undefined;
    vtable.outputInfo = Info.outputs;
    var controller: memory.AdmissionController = .{};
    const session = sessions.Session{ .ptr = undefined, .vtable = &vtable, .run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{ .scratch_limit_bytes = 256 * 1024 },
        .static_workspace_bytes = 0,
        .check_live_memory = false,
    } };
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var rejected = AdmittedAllocator{ .backing = failing.allocator(), .session = session };
    defer rejected.deinit();
    try std.testing.expectError(error.OutOfMemory, rejected.admitRun(.{ .workspace_bytes = 32 * 1024, .output_bytes = 0 }));
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
    var admitted = AdmittedAllocator{ .backing = std.testing.allocator, .session = session, .trim_on_free = true };
    defer admitted.deinit();
    var scope = try admitted.admitRun(.{ .workspace_bytes = 32 * 1024, .output_bytes = 0 });
    const a = admitted.allocator();
    const payload = try a.alloc(u8, 80 * 1024);
    try std.testing.expect(controller.snapshot().host_scratch_bytes >= admitted.live_bytes);
    try std.testing.expect(controller.snapshot().host_scratch_bytes < 128 * 1024);
    a.free(payload);
    scope.deinit();
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}

test "Native output escrow removes the exact prepaid subset while source allocations and final vectors survive independently" {
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    const Info = struct {
        fn outputs(_: *anyopaque) []const @import("tensor.zig").TensorInfo {
            return &.{};
        }
    };
    const domain = try Domain.create();
    defer domain.release();
    const pin = domain.pin();
    defer pin.release();
    var table: sessions.Session.VTable = undefined;
    table.outputInfo = Info.outputs;
    const session = sessions.Session{ .ptr = undefined, .vtable = &table, .run_admission = .{ .controller = &domain.controller, .backend_class = .cpu, .limits = .{ .scratch_limit_bytes = 256 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false } };
    var admitted = AdmittedAllocator{ .backing = std.testing.allocator, .session = session, .trim_on_free = true };
    defer admitted.deinit();
    var scope = try admitted.admitRun(.{ .workspace_bytes = 64 * 1024, .output_bytes = 0 });
    var scope_live = true;
    defer if (scope_live) scope.deinit();
    const a = admitted.allocator();
    const source = try a.alloc(u8, 4096);
    var source_live = true;
    defer if (source_live) a.free(source);
    const extra_source = try a.alloc(u8, 40 * 1024);
    const guarded = admitted.prepaid_host_bytes;
    const original_amounts = scope.permit.lease.?.amounts;
    try std.testing.expectError(error.HostOutputEscrowWouldUnfundSource, scope.prepareHostOutputsBeforeExecution(&pin, 2, 3000));
    try std.testing.expectEqual(guarded, admitted.prepaid_host_bytes);
    try std.testing.expectEqualDeep(original_amounts, scope.permit.lease.?.amounts);
    a.free(extra_source);
    const before = admitted.prepaid_host_bytes;
    const bytes = try sessions.HostOutputProducer.requiredBytes(2, 3);
    const quota = domain.controller.snapshot();
    var producer = try scope.prepareHostOutputsBeforeExecution(&pin, 2, 3);
    var producer_live = true;
    defer if (producer_live) {
        producer.abortWithCleanup(.{ .outside_execution = true }) catch unreachable;
        producer.finishOutsideExecution();
    };
    try std.testing.expectEqual(before - bytes, admitted.prepaid_host_bytes);
    try std.testing.expectEqualDeep(quota, domain.controller.snapshot());
    try producer.writeRow(0, &.{ 1, 2, 3 });
    try producer.writeRow(1, &.{ 4, 5, 6 });
    var vectors = try producer.publish();
    defer vectors.deinitOutsideExecution();
    producer.finishOutsideExecution();
    producer_live = false;
    scope.deinit();
    scope_live = false;
    try std.testing.expectEqual(@as(usize, 0), admitted.prepaid_host_bytes);
    try std.testing.expect(domain.controller.snapshot().host_scratch_bytes >= admitted.live_bytes + bytes);
    a.free(source);
    source_live = false;
    try admitted.trim();
    try std.testing.expectEqualSlices(f32, &.{ 4, 5, 6 }, vectors.values(1));
    try std.testing.expectEqual(bytes, domain.controller.snapshot().host_scratch_bytes);
    vectors.deinitOutsideExecution();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

fn hostOnlyGpuPrepaidConstructionCase(a: std.mem.Allocator) !void {
    const Lane = @import("../execution_control.zig").ExecutionLane;
    const Info = struct {
        fn outputs(_: *anyopaque) []const @import("tensor.zig").TensorInfo {
            return &.{};
        }
    };
    const Probe = struct {
        gate: *std.atomic.Mutex,
        lane: *Lane,
        calls: usize = 0,
        violation: bool = false,
        fn observe(self: *@This()) void {
            self.calls += 1;
            if (self.gate.tryLock()) self.gate.unlock() else self.violation = true;
            if (self.lane.mutex.tryLock()) {
                if (self.lane.owned) self.violation = true;
                self.lane.mutex.unlock();
            } else self.violation = true;
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            return 1;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
        }
    };
    var gate: std.atomic.Mutex = .unlocked;
    var lane: Lane = .{};
    var probe: Probe = .{ .gate = &gate, .lane = &lane };
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    defer controller.configureResourceBudget(null) catch @panic("prepaid metadata live quota");
    // SDK-free actual constructor layout. CUDA-only builds use a CPU value
    // layout to test the SAME GPU-class ledger/allocator path without fake SDK.
    const layout = if (comptime @import("build_options").enable_metal)
        try @import("compute_factory.zig").metalConstructionLayout()
    else
        @import("compute_factory.zig").HostConstructionLayout{ .bytes = 4096, .alignment = .of(u64) };
    const metadata = try AdmittedAllocator.constructionHostBytes(layout.bytes, layout.alignment);
    var table: sessions.Session.VTable = undefined;
    table.outputInfo = Info.outputs; // no numerical/backend callback is invoked
    const session: sessions.Session = .{ .ptr = &probe, .vtable = &table, .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = metadata + 64, .backend_limit_bytes = 2 * 1024 * 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false } };
    var owner: AdmittedAllocator = .{ .backing = a, .session = session, .trim_on_free = true };
    defer owner.deinit();
    const request: sessions.RunRequest = .{ .host_preprocess_bytes = metadata + 64, .workspace_bytes = 1024 * 1024 * 1024, .output_bytes = 0 };
    const floor = try session.run_admission.?.estimateRequest(request, &.{});
    var scope: AdmittedAllocator.RunScope = undefined;
    try owner.admitRunInto(&scope, request);
    defer scope.deinit();
    try std.testing.expectEqual(floor.host_scratch_bytes, owner.prepaid_host_bytes);
    try std.testing.expectEqualDeep(floor, controller.snapshot());
    try scope.beginNumerical();
    var numerical = true;
    defer if (numerical) scope.endNumerical();
    try std.testing.expect(gate.tryLock());
    var gate_held = true;
    defer if (gate_held) gate.unlock();
    var lease: ?Lane.Lease = try lane.acquire(.{});
    defer if (lease) |value| value.release();
    const before = probe.calls;
    const allocator = owner.allocator();
    const ptr = allocator.rawAlloc(layout.bytes, layout.alignment, @returnAddress()) orelse return error.OutOfMemory;
    var live = true;
    defer if (live) allocator.rawFree(ptr[0..layout.bytes], layout.alignment, @returnAddress());
    // Even enormous independently charged DEVICE workspace cannot satisfy a
    // missing HOST byte. The failure is before supplier/backing allocation.
    const too_big = owner.reserved_bytes + owner.prepaid_host_bytes + 1;
    try std.testing.expect(allocator.rawAlloc(too_big, .of(u8), @returnAddress()) == null);
    try std.testing.expectEqual(@as(?anyerror, error.ResourceLimitExceeded), owner.admission_error);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, owner.trim());
    try std.testing.expectEqual(before, probe.calls);
    allocator.rawFree(ptr[0..layout.bytes], layout.alignment, @returnAddress());
    live = false;
    try std.testing.expectEqual(before, probe.calls); // trim_on_free also cannot refund under owners
    try std.testing.expectEqualDeep(floor, controller.snapshot());
    lease.?.release();
    lease = null;
    gate.unlock();
    gate_held = false;
    scope.endNumerical();
    numerical = false;
    scope.deinit();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
    try std.testing.expect(!probe.violation);
}

test "GPU Source host-only constructor prepayment preserves device floors and every metadata OOM without supplier under Model or lane" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, hostOnlyGpuPrepaidConstructionCase, .{});
}

test "GPU Source exact constructor host class one below fails before supplier and cannot borrow backend budget" {
    const layout = if (comptime @import("build_options").enable_metal) try @import("compute_factory.zig").metalConstructionLayout() else @import("compute_factory.zig").HostConstructionLayout{ .bytes = 4096, .alignment = .of(u64) };
    const metadata = try AdmittedAllocator.constructionHostBytes(layout.bytes, layout.alignment);
    const Info = struct {
        fn outputs(_: *anyopaque) []const @import("tensor.zig").TensorInfo {
            return &.{};
        }
    };
    var table: sessions.Session.VTable = undefined;
    table.outputInfo = Info.outputs;
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    const session: sessions.Session = .{ .ptr = undefined, .vtable = &table, .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = metadata - 1, .backend_limit_bytes = 2 * 1024 * 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false } };
    var owner: AdmittedAllocator = .{ .backing = std.testing.allocator, .session = session };
    defer owner.deinit();
    var rejected: AdmittedAllocator.RunScope = undefined;
    try std.testing.expectError(error.ResourceLimitExceeded, owner.admitRunInto(&rejected, .{ .host_preprocess_bytes = metadata, .workspace_bytes = 1024 * 1024 * 1024, .output_bytes = 0 }));
    try std.testing.expect(owner.prepaid_host_bytes == 0 and owner.metadata_bytes == 0 and !owner.numerical_scope_active);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

fn gpuHostRetirementZeroSpareCase(a: std.mem.Allocator, enter: bool) !void {
    const Info = struct {
        fn outputs(_: *anyopaque) []const @import("tensor.zig").TensorInfo {
            return &.{};
        }
    };
    const Probe = struct {
        reject_retain: bool = false,
        calls: usize = 0,
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return self.calls;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.reject_retain) return error.ResourceTemporarilyUnavailable;
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
        }
    };
    var probe: Probe = .{};
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    defer controller.configureResourceBudget(null) catch @panic("class split quota leak");
    const payload = 1024;
    const host = try AdmittedAllocator.constructionHostBytes(payload, .of(u64));
    const device = 4096;
    var table: sessions.Session.VTable = undefined;
    table.outputInfo = Info.outputs;
    const policy: sessions.RunAdmission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = host, .backend_limit_bytes = device }, .static_workspace_bytes = 0, .check_live_memory = false };
    const session: sessions.Session = .{ .ptr = &probe, .vtable = &table, .run_admission = policy };
    var owner: AdmittedAllocator = .{ .backing = a, .session = session };
    defer owner.deinit();
    var scope: AdmittedAllocator.RunScope = undefined;
    try owner.admitRunInto(&scope, .{ .host_preprocess_bytes = host, .workspace_bytes = device, .output_bytes = 0 });
    defer scope.deinit();
    if (enter) try scope.beginNumerical();
    var numerical = enter;
    defer if (numerical) scope.endNumerical();
    const allocator = owner.allocator();
    const ptr = allocator.rawAlloc(payload, .of(u64), @returnAddress()) orelse return error.OutOfMemory;
    defer allocator.rawFree(ptr[0..payload], .of(u64), @returnAddress());
    try std.testing.expectEqual(host, owner.live_bytes + owner.metadata_bytes);
    try std.testing.expect(scope.host_retirement != null);
    // No class leaves the original exact source lease before SDK retirement.
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = host, .backend_scratch_bytes = device }, scope.permit.lease.?.amounts);
    scope.endNumerical();
    numerical = false;
    // Even a fallible external shrink cannot leave device ownership hidden in
    // the CPU reservation. Common SharedExternalAdmission retains retry custody.
    probe.reject_retain = true;
    scope.deinit();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = host }, controller.snapshot());
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = host }, owner.reservations.?.lease.?.amounts);
    var next = try controller.tryAcquire(.gpu, policy.limits, .{ .backend_scratch_bytes = device }, false);
    next.release();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = host }, controller.snapshot());
    probe.reject_retain = false;
}

test "GPU Source class retirement separates exact host-only custody with zero spare and refused external shrink before next device cohort" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, gpuHostRetirementZeroSpareCase, .{true});
    // Actual cold cancellation/error can retain tokenizer/input CPU payloads
    // without ever acquiring numerical Model/lane. Class custody is identical.
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, gpuHostRetirementZeroSpareCase, .{false});
}
