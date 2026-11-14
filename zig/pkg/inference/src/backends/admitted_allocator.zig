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
    metadata_bytes: usize = 0,
    admission_error: ?anyerror = null,
    blocks: ?*Header = null,
    reservations: ?*Reservation = null,

    const quantum = 64 * 1024;
    const Header = struct { prev: ?*Header, next: ?*Header, bytes: usize, alignment: std.mem.Alignment };
    const Reservation = struct { next: ?*Reservation, lease: ?memory.AdmissionLease, bytes: usize };
    pub const reservation_overhead = @sizeOf(Reservation);

    const vtable: std.mem.Allocator.VTable = .{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };

    pub fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    /// Only recognize this exact owner, never an arbitrary wrapping allocator.
    pub fn fromAllocator(value: std.mem.Allocator) ?*AdmittedAllocator {
        if (value.vtable != &vtable) return null;
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

    /// Move proven allocations and leases between synchronous owners sharing
    /// a backing heap and process domain, without copying data or reacquiring.
    pub fn absorb(self: *@This(), source: *@This()) !void {
        if (self == source or self.backing.ptr != source.backing.ptr or self.backing.vtable != source.backing.vtable or self.prepaid_host_bytes != 0 or source.prepaid_host_bytes != 0)
            return error.InvalidAdmissionLeaseReduction;
        const controller = if (self.session) |session| if (session.run_admission) |admission| admission.controller else null else null;
        if (source.reservations != null and controller != null and source.reservation_controller != controller) return error.InvalidAdmissionLeaseReduction;
        if (self.reservations != null and source.reservations != null and self.reservation_controller != source.reservation_controller) return error.InvalidAdmissionLeaseReduction;
        const live = try std.math.add(usize, self.live_bytes, source.live_bytes);
        const reserved = try std.math.add(usize, self.reserved_bytes, source.reserved_bytes);
        const metadata = try std.math.add(usize, self.metadata_bytes, source.metadata_bytes);
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
        self.live_bytes = live;
        self.reserved_bytes = reserved;
        self.metadata_bytes = metadata;
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

    fn reserve(self: *@This(), amount: usize) !?memory.AdmissionLease {
        std.debug.assert(self.admit == null or self.admit_owned == null);
        const owned = std.math.add(usize, self.reserved_bytes, self.prepaid_host_bytes) catch return error.ResourceLimitExceeded;
        if (self.admit_owned) |callback| return try callback(self, amount);
        if (self.admit) |callback| return try callback(self.admission_context.?, amount);
        const session = self.session orelse return error.ResourceLimitExceeded;
        const inherited = if (self.parent) |parent| if (session.run_admission) |admission| try parent.ownedHostBytesFor(admission.controller) else 0 else 0;
        return (try session.admitHostPreprocessOwnedWithResidency(amount, try std.math.add(usize, owned, inherited), if (session.run_admission) |admission| try self.requiredResidencyFor(admission.controller) else .{})).lease;
    }

    /// Execution borrows this permit's workspace. On exit, adopt its lease
    /// before returning unused capacity, so surviving outputs never lose their
    /// admission and no fresh reservation is required during cleanup.
    pub const RunScope = struct {
        permit: sessions.RunPermit,
        owner: ?*AdmittedAllocator = null,
        reservation: ?*Reservation = null,

        pub fn deinit(self: *@This()) void {
            if (self.owner) |owner| {
                const node = self.reservation.?;
                node.* = .{ .next = owner.reservations, .lease = self.permit.lease, .bytes = owner.prepaid_host_bytes };
                owner.reservations = node;
                owner.reservation_controller = if (self.permit.lease) |lease| lease.controller else null;
                owner.reserved_bytes += owner.prepaid_host_bytes;
                owner.prepaid_host_bytes = 0;
                self.permit.lease = null;
                // A failed external reduction conservatively keeps its lease.
                // Later phases retry trim; final teardown always releases it.
                owner.trim() catch {};
            }
            self.permit.deinit();
            self.owner = null;
        }
    };

    pub fn admitRun(self: *@This(), request: sessions.RunRequest) !RunScope {
        const session = self.session orelse return error.InvalidAdmissionLeaseReduction;
        const admission = session.run_admission orelse return .{ .permit = try session.admit(request) };
        if (self.reservations != null and self.reservation_controller != admission.controller) return error.InvalidAdmissionLeaseReduction;
        if (self.admit != null or self.admit_owned != null or self.prepaid_host_bytes != 0 or admission.backend_class != .cpu) return error.InvalidAdmissionLeaseReduction;
        const amounts = try admission.estimateRequest(request, session.outputInfo());
        if (amounts.host_kv_bytes != 0) return error.InvalidAdmissionLeaseReduction;
        if (amounts.host_scratch_bytes < @sizeOf(Reservation)) return .{ .permit = try session.admit(request) };
        try self.trim();
        try admission.validateOwnedHostScratch(std.math.add(usize, self.reserved_bytes, amounts.host_scratch_bytes) catch return error.ResourceLimitExceeded);
        var permit = try session.admit(request);
        errdefer permit.deinit();
        // The execution lease covers this node too. Allocate it now so cleanup
        // can transfer retained outputs without allocation or reacquisition.
        const node = try self.backing.create(Reservation);
        self.metadata_bytes += @sizeOf(Reservation);
        self.prepaid_host_bytes = amounts.host_scratch_bytes;
        return .{ .permit = permit, .owner = self, .reservation = node };
    }

    pub fn ensureCapacity(self: *@This(), extra: usize) !void {
        const needed = try std.math.add(usize, try std.math.add(usize, self.live_bytes, self.metadata_bytes), extra);
        const capacity = try std.math.add(usize, self.reserved_bytes, self.prepaid_host_bytes);
        if (needed <= capacity) return;
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
