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
    /// Session-based callbacks must read this allocator's current session,
    /// which carries the attempt-local diagnostic sink during growth retries.
    admit: ?*const fn (*anyopaque, usize) anyerror!memory.AdmissionLease = null,
    /// Custom process admission with cumulative owner residency, charged only
    /// for the new delta. Mutually exclusive with the independent-ledger hook.
    admit_owned: ?*const fn (*const AdmittedAllocator, usize) anyerror!memory.AdmissionLease = null,
    /// Refund auxiliary ledgers when a successful custom admission cannot be
    /// adopted because reservation metadata allocation fails.
    rollback_admission: ?*const fn (*anyopaque, usize) void = null,
    /// Borrowed synchronous ancestry; never moved into a published result.
    parent: ?*const AdmittedAllocator = null,
    trim_on_free: bool = false,
    live_bytes: usize = 0,
    reserved_bytes: usize = 0,
    /// Reservations in an owner belong to one ledger. This makes ancestry
    /// queries constant-time even after hundreds of regional result transfers.
    reservation_controller: ?*memory.AdmissionController = null,
    residency_domain: ?*memory.AdmissionController = null,
    required_residency: memory.Residency = .{},
    residency_bindings: usize = 0,
    unbound_residency_domain: ?*memory.AdmissionController = null,
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
    /// One real serving-resource owner, shared by every chunk and adopted run
    /// lease. It outlives all external reservation callbacks, even Model close.
    resource_pin: ?sessions.Session.ModelLifetime.ResourcePin = null,

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

    /// Only recognize this exact owner, never an arbitrary wrapping allocator.
    pub fn fromAllocator(value: std.mem.Allocator) ?*AdmittedAllocator {
        if (value.vtable != &allocator_vtable) return null;
        return @ptrCast(@alignCast(value.ptr));
    }

    /// Switch from load-time metadata admission to a loaded session without
    /// releasing live allocations. Every retained lease must belong to the
    /// same process ledger; custom rollback ledgers cannot be transferred.
    pub fn bindSession(self: *@This(), session: sessions.Session) !void {
        const admission = session.run_admission orelse return error.InvalidAdmissionLeaseReduction;
        if (self.prepaid_host_bytes != 0 or self.rollback_admission != null or admission.backend_class != .cpu) return error.InvalidAdmissionLeaseReduction;
        var reservation = self.reservations;
        while (reservation) |node| : (reservation = node.next) {
            const lease = node.lease orelse return error.InvalidAdmissionLeaseReduction;
            if (lease.controller != admission.controller) return error.InvalidAdmissionLeaseReduction;
        }
        try self.validateResourceOwner(session);
        if (self.resource_pin == null) {
            if (session.model_lifetime) |lifetime| if (lifetime.resource_pin) |pin| {
                self.resource_pin = pin.retain() orelse return error.ForeignAdmissionResourceOwner;
            };
        }
        self.session = session;
        self.admit = null;
        self.admit_owned = null;
        self.admission_context = null;
    }

    /// Only plain CPU scratch can share this allocation ledger. KV outputs,
    /// foreign domains and custom rollback ledgers retain execution ownership.
    pub fn canBorrowRun(self: *const @This(), session: sessions.Session, request: sessions.RunRequest) !bool {
        const admission = session.run_admission orelse return false;
        if (admission.backend_class != .cpu or self.admit != null or self.admit_owned != null or self.rollback_admission != null or self.parent != null or self.prepaid_host_bytes != 0) return false;
        var reservation = self.reservations;
        while (reservation) |node| : (reservation = node.next) {
            const lease = node.lease orelse return false;
            if (lease.controller != admission.controller or lease.retain_backend_class != .cpu) return false;
        }
        return (try admission.estimateRequest(request, session.outputInfo())).host_kv_bytes == 0;
    }

    /// A live payload range owned by this allocator, excluding bookkeeping.
    pub fn ownsRange(self: *const @This(), bytes: []const u8) bool {
        if (bytes.len == 0) return true;
        const address = @intFromPtr(bytes.ptr);
        var block = self.blocks;
        while (block) |entry| : (block = entry.next) {
            const base = @intFromPtr(entry);
            if (address >= base + @sizeOf(Header) and address - base <= entry.bytes and bytes.len <= entry.bytes - (address - base)) return true;
        }
        return false;
    }

    pub fn ownedHostBytesFor(self: *const @This(), controller: *memory.AdmissionController) !usize {
        var bytes: usize = if (self.reservation_controller == controller) self.reserved_bytes else 0;
        if (self.session) |session| if (session.run_admission) |admission| if (admission.controller == controller) {
            bytes = try std.math.add(usize, bytes, self.prepaid_host_bytes);
        };
        if (self.parent) |parent| bytes = try std.math.add(usize, bytes, try parent.ownedHostBytesFor(controller));
        return bytes;
    }

    pub fn requiredResidencyFor(self: *const @This(), controller: *memory.AdmissionController) !memory.Residency {
        const own = if (self.residency_domain == controller) self.required_residency else memory.Residency{};
        return if (self.parent) |parent| try own.merge(try parent.requiredResidencyFor(controller)) else own;
    }

    pub const ResidencyBinding = struct {
        owner: ?*AdmittedAllocator,
        addition: memory.Residency,
        pub fn deinit(self: *@This()) void {
            const owner = self.owner orelse return;
            for (&owner.required_residency.by_backend, self.addition.by_backend) |*held, amount| {
                inline for (@typeInfo(memory.AdmissionAmounts).@"struct".field_names) |name| {
                    std.debug.assert(@field(held, name) >= @field(amount, name));
                    @field(held, name) -= @field(amount, name);
                }
            }
            owner.residency_bindings -= 1;
            if (owner.residency_bindings == 0) owner.residency_domain = owner.unbound_residency_domain;
            self.owner = null;
        }
    };

    /// Borrow pinned resources for synchronous allocation feasibility. Bindings
    /// subtract only their own contribution and may be released in any order.
    pub fn requireResidency(self: *@This(), controller: *memory.AdmissionController, residency: memory.Residency) !ResidencyBinding {
        if (self.residency_domain != null and self.residency_domain != controller) return error.InvalidAdmissionLeaseReduction;
        const total = try self.required_residency.merge(residency);
        const count = try std.math.add(usize, self.residency_bindings, 1);
        if (self.residency_bindings == 0) self.unbound_residency_domain = self.residency_domain;
        self.required_residency = total;
        self.residency_bindings = count;
        self.residency_domain = controller;
        return .{ .owner = self, .addition = residency };
    }

    const AbsorbTotals = struct { live: usize, reserved: usize, metadata: usize };

    fn validateAbsorb(self: *const @This(), source: *const @This(), source_domain: ?*memory.AdmissionController) !AbsorbTotals {
        if (self == source or self.backing.ptr != source.backing.ptr or self.backing.vtable != source.backing.vtable or self.prepaid_host_bytes != 0 or source.prepaid_host_bytes != 0)
            return error.InvalidAdmissionLeaseReduction;
        const controller = if (self.session) |session| if (session.run_admission) |admission| admission.controller else null else null;
        if (source.reservations != null and controller != null and source_domain != controller) return error.InvalidAdmissionLeaseReduction;
        if (self.reservations != null and source.reservations != null and self.reservation_controller != source_domain) return error.InvalidAdmissionLeaseReduction;
        // Moved leases keep their actual resource owner alive after Model close.
        // Validate before mutation; duplicate pins settle with their original
        // empty owner rather than calling an external release in this transfer.
        if (source.resource_pin) |pin| if (self.resource_pin) |held| {
            if (!std.meta.eql(held, pin)) return error.ForeignAdmissionResourceOwner;
        };
        if (self.session) |session| {
            try self.validateResourceOwner(session);
            try source.validateResourceOwner(session);
        }
        return .{
            .live = try std.math.add(usize, self.live_bytes, source.live_bytes),
            .reserved = try std.math.add(usize, self.reserved_bytes, source.reserved_bytes),
            .metadata = try std.math.add(usize, self.metadata_bytes, source.metadata_bytes),
        };
    }

    /// Move proven allocations and leases without copying or reacquiring.
    /// Every rejection leaves both owners unchanged.
    pub fn absorb(self: *@This(), source: *@This()) !void {
        const totals = try self.validateAbsorb(source, source.reservation_controller);
        self.commitAbsorb(source, totals);
    }

    /// Offline ledgers are embedded in their scopes and cannot escape them.
    /// Acquire one replacement lease before mutating either owner, then
    /// consolidate local reservations and commit without fallible operations.
    pub fn absorbLocal(self: *@This(), source: *@This(), target_domain: *memory.AdmissionController, source_domain: *memory.AdmissionController) !void {
        var totals = try self.validateAbsorb(source, target_domain);
        var reservation = source.reservations;
        while (reservation) |node| : (reservation = node.next) {
            const lease = node.lease orelse return error.InvalidAdmissionLeaseReduction;
            if (lease.controller != source_domain or lease.hasExternalReceipt() or lease.live_reserved_bytes != 0 or lease.live_settled_bytes != 0 or lease.retain_backend_class != .cpu or lease.amounts.host_scratch_bytes != node.bytes or lease.amounts.hostTotalBytes() != node.bytes or lease.amounts.backendTotalBytes() != 0)
                return error.InvalidAdmissionLeaseReduction;
        }
        if (source.reservations) |first| {
            const replacement = try target_domain.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = source.reserved_bytes }, false);
            var next = first.next;
            first.lease.?.release();
            first.lease = replacement;
            first.bytes = source.reserved_bytes;
            first.next = null;
            while (next) |node| {
                next = node.next;
                node.lease.?.release();
                source.backing.destroy(node);
            }
            totals.metadata -= source.metadata_bytes - @sizeOf(Reservation);
            source.metadata_bytes = @sizeOf(Reservation);
            source.reservation_controller = target_domain;
        }
        self.commitAbsorb(source, totals);
    }

    fn commitAbsorb(self: *@This(), source: *@This(), totals: AbsorbTotals) void {
        if (self.reservations == null) self.reservation_controller = source.reservation_controller;
        if (source.blocks) |head| {
            var tail = head;
            while (tail.next) |next| tail = next;
            tail.next = self.blocks;
            if (self.blocks) |previous| previous.prev = tail;
            self.blocks = head;
        }
        if (source.reservations) |head| {
            var tail = head;
            while (tail.next) |next| tail = next;
            tail.next = self.reservations;
            self.reservations = head;
        }
        if (self.resource_pin == null) {
            self.resource_pin = source.resource_pin;
            source.resource_pin = null;
        }
        self.live_bytes = totals.live;
        self.reserved_bytes = totals.reserved;
        self.metadata_bytes = totals.metadata;
        source.blocks = null;
        source.reservations = null;
        source.live_bytes = 0;
        source.reserved_bytes = 0;
        source.reservation_controller = null;
        source.metadata_bytes = 0;
    }

    pub fn allocationOverhead(alignment: std.mem.Alignment) usize {
        return std.mem.alignForward(usize, @sizeOf(Header), alignment.toByteUnits());
    }

    fn validateResourceOwner(self: *const @This(), session: sessions.Session) !void {
        const held = self.resource_pin orelse return;
        const lifetime = session.model_lifetime orelse return error.ForeignAdmissionResourceOwner;
        const current = lifetime.resource_pin orelse return error.ForeignAdmissionResourceOwner;
        if (!std.meta.eql(held, current)) return error.ForeignAdmissionResourceOwner;
    }

    fn retainRunResources(self: *@This(), permit: *const sessions.RunPermit) !void {
        const pin = permit.resource_pin orelse return;
        if (self.resource_pin) |held| {
            if (!std.meta.eql(held, pin)) return error.ForeignAdmissionResourceOwner;
        } else self.resource_pin = pin.retain() orelse return error.ForeignAdmissionResourceOwner;
    }

    fn reserve(self: *@This(), amount: usize) !?memory.AdmissionLease {
        std.debug.assert(self.admit == null or self.admit_owned == null);
        const owned = std.math.add(usize, self.reserved_bytes, self.prepaid_host_bytes) catch return error.ResourceLimitExceeded;
        if (self.admit_owned) |callback| return try callback(self, amount);
        if (self.admit) |callback| return try callback(self.admission_context.?, amount);
        const session = self.session orelse return error.ResourceLimitExceeded;
        try self.validateResourceOwner(session);
        // Separate chunk leases still belong to one request. Its own retained
        // bytes cannot turn an intrinsically oversized request into contention.
        if (session.run_admission) |admission| {
            const total = std.math.add(usize, owned, amount) catch return error.ResourceLimitExceeded;
            try admission.validateOwnedHostScratch(total);
        }
        const inherited = if (self.parent) |parent| if (session.run_admission) |admission| try parent.ownedHostBytesFor(admission.controller) else 0 else 0;
        var permit = try session.admitHostPreprocessOwnedWithResidency(amount, try std.math.add(usize, owned, inherited), if (session.run_admission) |admission| try self.requiredResidencyFor(admission.controller) else .{});
        errdefer permit.deinit();
        if (permit.resource_pin) |pin| {
            if (self.resource_pin) |held| {
                if (!std.meta.eql(held, pin)) return error.ForeignAdmissionResourceOwner;
                permit.resource_pin = null;
                pin.release(); // Duplicate chunk retain; the AA owns one pin.
            } else {
                self.resource_pin = pin;
                permit.resource_pin = null;
            }
        }
        const lease = permit.lease;
        permit.lease = null;
        return lease;
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
                owner.reservation_controller = if (self.permit.lease) |lease| lease.controller else null;
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
        try self.validateResourceOwner(session);
        const admission = session.run_admission orelse {
            if (task_output != null) return error.UnmanagedHostOutput;
            return .{ .permit = try session.admit(request) };
        };
        if (self.reservations != null and self.reservation_controller != admission.controller) return error.InvalidAdmissionLeaseReduction;
        if (self.admit != null or self.admit_owned != null or self.prepaid_host_bytes != 0) return error.InvalidAdmissionLeaseReduction;
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
        try self.retainRunResources(&permit);
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
        const original_session = self.session;
        var failure = memory.AdmissionFailure.Capture.init(if (original_session) |session| if (session.run_admission) |admission| admission.failure else null else null);
        if (original_session) |session| self.session = session.withAdmissionFailure(failure.sink());
        defer self.session = original_session;
        return self.reserveGrowth(minimum) catch |err| {
            failure.publish(err);
            return err;
        };
    }

    fn reserveGrowth(self: *@This(), minimum: usize) !void {
        var amount = @max(minimum, quantum);
        var lease = self.reserve(amount) catch |err| blk: {
            if (amount == minimum or (err != error.ResourceLimitExceeded and err != error.ResourceTemporarilyUnavailable and err != error.MemoryBudgetExceeded)) return err;
            amount = minimum; // Slack must not reject requests fitting tight limits.
            break :blk try self.reserve(amount);
        };
        errdefer {
            if (lease) |*active| active.release();
            if (self.rollback_admission) |rollback| rollback(self.admission_context.?, amount);
        }
        // This allocator owns host scratch only. A callback cannot offer a
        // smaller or differently classified lease as allocation credit.
        if (lease) |active| if (active.amounts.host_scratch_bytes != amount or active.amounts.hostTotalBytes() != amount or active.amounts.backendTotalBytes() != 0) return error.InvalidAdmissionLeaseReduction;
        const domain = if (lease) |active| active.controller else null;
        if (self.reservations != null and self.reservation_controller != domain) return error.InvalidAdmissionLeaseReduction;
        const reservation = try self.backing.create(Reservation);
        self.reservation_controller = domain;
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
            self.reservation_controller = null;
            return;
        }
        var link = &self.reservations;
        while (link.*) |reservation| {
            const spare = (self.reserved_bytes + self.prepaid_host_bytes) -| (self.live_bytes + self.metadata_bytes);
            if (spare == 0) break;
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
        self.reservation_controller = null;
        self.metadata_bytes = 0;
        const pin = self.resource_pin;
        self.resource_pin = null;
        if (pin) |held| held.release();
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
            var scope = try admitted.admitRun(try @import("../execution/embedding_gemma2_admission.zig").nativeTextEncoderRequest(4096, rows, 768));
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

fn admittedResourceLifetimeCase(a: std.mem.Allocator, prepaid: bool) !void {
    const Probe = struct {
        controller: *memory.AdmissionController,
        refs: usize = 1,
        model_closed: bool = false,
        final_releases: usize = 0,
        callbacks: usize = 0,
        violation: bool = false,
        fn pinRetain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.refs == 0) self.violation = true;
            self.refs += 1;
        }
        fn pinRelease(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.refs == 0) @panic("double resource release");
            self.refs -= 1;
            if (self.refs == 0) {
                self.final_releases += 1;
                if (!self.model_closed or self.controller.snapshot().hostTotalBytes() != 0) self.violation = true;
            }
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.model_closed = true;
        }
        fn outputs(_: *anyopaque) []const @import("tensor.zig").TensorInfo {
            return &.{};
        }
        fn observe(self: *@This()) void {
            self.callbacks += 1;
            if (self.refs == 0) self.violation = true;
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            return 1;
        }
        fn resizeLease(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
        }
        fn releaseLease(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
        }
    };
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    var probe = Probe{ .controller = &controller };
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.resizeLease, .release = Probe.releaseLease });
    defer controller.configureResourceBudget(null) catch @panic("resource lifetime quota still live");
    defer if (probe.refs != 0 or probe.final_releases != 1 or probe.violation) @panic("resource lifetime or failure cleanup violated");
    var lifetime = sessions.Session.ModelLifetime{ .owner = &probe, .destroy_fn = Probe.close, .resource_pin = .{ .ptr = &probe, .retain_fn = Probe.pinRetain, .release_fn = Probe.pinRelease } };
    var table: sessions.Session.VTable = undefined;
    table.outputInfo = Probe.outputs;
    const session = sessions.Session{ .ptr = &probe, .vtable = &table, .model_lifetime = &lifetime, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false } };
    var model_live = true;
    defer if (model_live) session.close();
    var owner = AdmittedAllocator{ .backing = a, .session = session };
    var owner_live = true;
    defer if (owner_live) owner.deinit();
    var allocations: [2]?[]u8 = @splat(null);
    defer for (allocations) |allocation| if (allocation) |bytes| owner.allocator().free(bytes);
    if (prepaid) {
        var scope = try owner.admitRun(.{ .host_preprocess_bytes = 256 * 1024, .workspace_bytes = 0, .output_bytes = 0 });
        defer scope.deinit();
        allocations[0] = try owner.allocator().alloc(u8, 70000);
        allocations[1] = try owner.allocator().alloc(u8, 70000);
        try std.testing.expectEqual(@as(usize, 3), probe.refs); // Model, AA, live permit.
    } else {
        allocations[0] = try owner.allocator().alloc(u8, 70000);
        allocations[1] = try owner.allocator().alloc(u8, 70000);
    }
    try std.testing.expectEqual(@as(usize, 2), probe.refs); // One AA pin across all chunks.
    try std.testing.expect(controller.snapshot().hostTotalBytes() != 0);
    // A live allocator cannot silently switch the resources behind its leases.
    var foreign_probe = Probe{ .controller = &controller };
    var foreign_lifetime = sessions.Session.ModelLifetime{ .owner = &foreign_probe, .destroy_fn = Probe.close, .resource_pin = .{ .ptr = &foreign_probe, .retain_fn = Probe.pinRetain, .release_fn = Probe.pinRelease } };
    var foreign_session = session;
    foreign_session.model_lifetime = &foreign_lifetime;
    owner.session = foreign_session;
    try std.testing.expectError(error.ForeignAdmissionResourceOwner, owner.ensureCapacity(owner.reserved_bytes + 1));
    owner.session = session;
    try std.testing.expectEqual(@as(usize, 1), foreign_probe.refs);
    session.close();
    model_live = false;
    try std.testing.expect(probe.model_closed);
    try std.testing.expectEqual(@as(usize, 1), probe.refs);
    try std.testing.expectEqual(@as(usize, 0), probe.final_releases);
    for (&allocations) |*allocation| {
        owner.allocator().free(allocation.*.?);
        allocation.* = null;
    }
    const before_release = probe.callbacks;
    owner.deinit();
    owner_live = false;
    try std.testing.expect(probe.callbacks > before_release);
    try std.testing.expectEqual(@as(usize, 0), probe.refs);
    try std.testing.expectEqual(@as(usize, 1), probe.final_releases);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().hostTotalBytes());
    try std.testing.expect(!probe.violation);
}

test "admitted allocator keeps actual serving resources alive through chunk callbacks after model close" {
    try admittedResourceLifetimeCase(std.testing.allocator, false);
    try admittedResourceLifetimeCase(std.testing.allocator, true);
}

test "admitted allocator serving resource pins unwind every cold allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, admittedResourceLifetimeCase, .{false});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, admittedResourceLifetimeCase, .{true});
}

test "admitted allocator admission includes cumulative growth and resident weights" {
    var controller = memory.AdmissionController{};
    const limits = memory.Limits{ .host_limit_bytes = 100 * 1024 };
    var weights = try controller.tryAcquire(.cpu, limits, .{ .host_weight_bytes = 90 * 1024 }, false);
    defer weights.release();
    const session = sessions.Session{ .ptr = &controller, .vtable = undefined, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = limits, .static_workspace_bytes = 0, .required_residency = weights.amounts, .check_live_memory = false } };
    var owner = AdmittedAllocator{ .backing = std.testing.allocator, .session = session };
    defer owner.deinit();
    const alloc = owner.allocator();
    const first = try alloc.alloc(u8, 6 * 1024);
    @memset(first, 17);
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 6 * 1024));
    try std.testing.expectEqual(error.ResourceLimitExceeded, owner.admission_error.?);
    try std.testing.expectEqual(@as(u8, 17), first[0]);
    alloc.free(first);
    try owner.trim();
    try std.testing.expectEqualDeep(weights.amounts, controller.snapshot());
}

test "admission growth discards recovered slack diagnostics and replaces unknown terminal failures" {
    const Provider = struct {
        deny: bool = false,
        next: usize = 1,
        fn acquire(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.deny) return error.ResourceLimitExceeded;
            defer self.next += 1;
            return self.next;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(_: *anyopaque, _: usize) void {}
    };
    var provider = Provider{};
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    try controller.configureResourceBudget(.{ .context = &provider, .try_reserve = Provider.acquire, .retain = Provider.retain, .release = Provider.release });
    const limits = memory.Limits{ .host_limit_bytes = 100 * 1024 };
    var weights = try controller.tryAcquire(.cpu, limits, .{ .host_weight_bytes = 90 * 1024 }, false);
    defer weights.release();
    var failure = memory.AdmissionFailure{};
    const session = sessions.Session{ .ptr = &controller, .vtable = undefined, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = limits, .static_workspace_bytes = 0, .required_residency = weights.amounts, .failure = &failure, .check_live_memory = false } };
    var owner = AdmittedAllocator{ .backing = std.testing.allocator, .session = session };
    defer owner.deinit();
    const a = owner.allocator();
    const first = try a.alloc(u8, 512);
    try std.testing.expect(failure.snapshot() == null);
    a.free(first);
    try owner.trim();
    // A successful retry must not clear another operation's terminal result.
    const peer = memory.AdmissionAmounts.LimitExceeded{ .limit = .backend, .requested_bytes = 12, .limit_bytes = 10 };
    failure.record(peer);
    const second = try a.alloc(u8, 512);
    defer a.free(second);
    try std.testing.expectEqualDeep(peer, failure.snapshot().?);
    provider.deny = true;
    try std.testing.expectError(error.ResourceLimitExceeded, session.admitHostPreprocess(1));
    try std.testing.expect(failure.snapshot() == null);
    // The current attempt's real policy constraint replaces unknown details.
    try std.testing.expectError(error.ResourceLimitExceeded, session.admitHostPreprocess(20 * 1024));
    try std.testing.expectEqualDeep(memory.AdmissionAmounts.LimitExceeded{ .limit = .host, .requested_bytes = 110 * 1024, .limit_bytes = 100 * 1024 }, failure.snapshot().?);
}

test "CPU media workspace binds only its own ledger and returns encoder credit before decoding" {
    const Provider = struct {
        controller: memory.AdmissionController = .{},
        fn acquire(raw: *anyopaque, bytes: usize) !memory.AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.controller.tryAcquire(.cpu, .{ .scratch_limit_bytes = 512 * 1024 }, .{ .host_scratch_bytes = bytes }, false);
        }
        fn outputs(_: *anyopaque) []const @import("tensor.zig").TensorInfo {
            return &.{};
        }
    };
    var provider = Provider{};
    var other: memory.AdmissionController = .{};
    var vtable_: sessions.Session.VTable = undefined;
    vtable_.outputInfo = Provider.outputs;
    var session = sessions.Session{ .ptr = undefined, .vtable = &vtable_, .run_admission = .{ .controller = &other, .backend_class = .cpu, .limits = .{ .scratch_limit_bytes = 512 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false } };
    var workspace = AdmittedAllocator{ .backing = std.testing.allocator, .admission_context = &provider, .admit = Provider.acquire, .trim_on_free = true };
    defer workspace.deinit();
    const a = workspace.allocator();
    try std.testing.expect(AdmittedAllocator.fromAllocator(a) == &workspace);
    try std.testing.expect(AdmittedAllocator.fromAllocator(std.testing.allocator) == null);
    const metadata = try a.alloc(u8, 128);
    defer a.free(metadata);
    try std.testing.expectError(error.InvalidAdmissionLeaseReduction, workspace.bindSession(session));
    try std.testing.expect(workspace.admit != null);
    session.run_admission.?.controller = &provider.controller;
    try workspace.bindSession(session);
    var scope = try workspace.admitRun(.{ .workspace_bytes = 256 * 1024, .output_bytes = 0 });
    defer scope.deinit();
    const prepaid_total = provider.controller.snapshot().host_scratch_bytes;
    const output = try a.alloc(u8, 32 * 1024);
    const transient = try a.alloc(u8, 128 * 1024);
    // Physical allocations consume the estimate, rather than adding it again.
    try std.testing.expectEqual(prepaid_total, provider.controller.snapshot().host_scratch_bytes);
    a.free(transient);
    scope.deinit();
    try std.testing.expectEqual(@as(usize, 0), workspace.prepaid_host_bytes);
    try std.testing.expect(provider.controller.snapshot().host_scratch_bytes < 64 * 1024);
    @memset(output, 42);
    for (output) |value| try std.testing.expectEqual(@as(u8, 42), value);
    a.free(output);
    try workspace.trim();
    try std.testing.expect(provider.controller.snapshot().host_scratch_bytes < 4096);
}

test "reader admitted owner caches domain totals across regional transfers and trimming" {
    var controller = memory.AdmissionController{};
    var foreign = memory.AdmissionController{};
    const session = sessions.Session{ .ptr = undefined, .vtable = undefined, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 1, .check_live_memory = false } };
    var target = AdmittedAllocator{ .backing = std.testing.allocator, .session = session };
    defer target.deinit();
    var pointers: [128][]u8 = undefined;
    for (&pointers) |*bytes| {
        var source = AdmittedAllocator{ .backing = std.testing.allocator, .session = session };
        defer source.deinit();
        bytes.* = try source.allocator().alloc(u8, 16);
        try source.trim();
        try target.absorb(&source);
        try std.testing.expectEqual(target.reserved_bytes, try target.ownedHostBytesFor(&controller));
        try std.testing.expectEqual(@as(usize, 0), try target.ownedHostBytesFor(&foreign));
        try target.trim();
    }
    for (pointers) |bytes| target.allocator().free(bytes);
    try target.trim();
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().hostTotalBytes());
    try std.testing.expect(target.reservation_controller == null);
}

test "reader pinned residency binding restores nested ownership and rejects foreign domains" {
    var controller = memory.AdmissionController{};
    var foreign = memory.AdmissionController{};
    const session = sessions.Session{ .ptr = undefined, .vtable = undefined, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 16 * 1024, .scratch_limit_bytes = 16 * 1024 }, .static_workspace_bytes = 1, .check_live_memory = false } };
    var weights = try controller.tryAcquire(.cpu, .{}, .{ .host_weight_bytes = 12 * 1024 }, false);
    defer weights.release();
    var owner = AdmittedAllocator{ .backing = std.testing.allocator, .session = session };
    defer owner.deinit();
    var binding = try owner.requireResidency(&controller, .fromLease(weights));
    try std.testing.expectError(error.OutOfMemory, owner.allocator().alloc(u8, 8 * 1024));
    try std.testing.expectEqual(error.ResourceLimitExceeded, owner.admission_error.?);
    try std.testing.expectError(error.InvalidAdmissionLeaseReduction, owner.requireResidency(&foreign, .{}));
    var sibling = try owner.requireResidency(&controller, .fromAmounts(.cpu, .{ .host_weight_bytes = 1 }));
    binding.deinit();
    try std.testing.expectEqual(@as(usize, 1), (try owner.required_residency.total()).host_weight_bytes);
    sibling.deinit();
    try std.testing.expect(owner.residency_domain == null);
    try std.testing.expectEqualDeep(memory.Residency{}, owner.required_residency);
}

test "reader admitted owner rejects malformed callback credit before backing allocation" {
    const Provider = struct {
        controller: memory.AdmissionController = .{},
        fn acquire(raw: *anyopaque, bytes: usize) !memory.AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = bytes / 2 }, false);
        }
    };
    var provider = Provider{};
    var owner = AdmittedAllocator{ .backing = std.testing.failing_allocator, .admission_context = &provider, .admit = Provider.acquire };
    defer owner.deinit();
    try std.testing.expectError(error.OutOfMemory, owner.allocator().alloc(u8, 1));
    try std.testing.expectEqual(error.InvalidAdmissionLeaseReduction, owner.admission_error.?);
    try std.testing.expectEqual(@as(usize, 0), provider.controller.snapshot().hostTotalBytes());
}

test "Gemma4 CUDA host PLE shared receipts cannot be rehomed as local allocator credit" {
    const Mock = struct {
        fn reserve(_: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            return 17;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(_: *anyopaque, _: usize) void {}
    };
    var marker: u8 = 0;
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    try controller.configureResourceBudget(.{ .context = &marker, .try_reserve = Mock.reserve, .retain = Mock.retain, .release = Mock.release });
    var foreign = memory.AdmissionController{};
    defer foreign.deinit();
    const session = sessions.Session{ .ptr = undefined, .vtable = undefined, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 1, .check_live_memory = false } };
    var source = AdmittedAllocator{ .backing = std.testing.allocator, .session = session };
    defer source.deinit();
    const bytes = try source.allocator().alloc(u8, 16);
    defer source.allocator().free(bytes);
    const node = source.reservations.?;
    try std.testing.expect(node.next == null);
    var empty_parent = node.lease.?;
    node.lease = try empty_parent.split(empty_parent.amounts);
    defer empty_parent.release();
    const shared = node.lease.?;
    // Every ordinary local-transfer precondition still holds. Only the shared
    // external receipt must prevent rehoming these real allocated bytes.
    try std.testing.expect(shared.external_lease == null and shared.hasExternalReceipt());
    try std.testing.expect(shared.live_reserved_bytes == 0 and shared.live_settled_bytes == 0 and shared.retain_backend_class == .cpu);
    try std.testing.expectEqual(node.bytes, shared.amounts.host_scratch_bytes);
    try std.testing.expectEqual(node.bytes, shared.amounts.hostTotalBytes());
    try std.testing.expectEqual(@as(usize, 0), shared.amounts.backendTotalBytes());
    var target = AdmittedAllocator{ .backing = std.testing.allocator };
    defer target.deinit();
    const before = controller.snapshot();
    try std.testing.expectError(error.InvalidAdmissionLeaseReduction, target.absorbLocal(&source, &foreign, &controller));
    try std.testing.expectEqual(before, controller.snapshot());
    try std.testing.expectEqual(memory.AdmissionAmounts{}, foreign.snapshot());
    try std.testing.expectEqual(@as(usize, 0), target.live_bytes);
}
