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
    /// Refund auxiliary ledgers when a successful custom admission cannot be
    /// adopted because reservation metadata allocation fails.
    rollback_admission: ?*const fn (*anyopaque, usize) void = null,
    trim_on_free: bool = false,
    live_bytes: usize = 0,
    reserved_bytes: usize = 0,
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

    pub fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    pub fn allocationOverhead(alignment: std.mem.Alignment) usize {
        return std.mem.alignForward(usize, @sizeOf(Header), alignment.toByteUnits());
    }

    fn reserve(self: *@This(), amount: usize) !?memory.AdmissionLease {
        if (self.admit) |callback| return try callback(self.admission_context.?, amount);
        const session = self.session orelse return error.ResourceLimitExceeded;
        const owned = std.math.add(usize, self.reserved_bytes, self.prepaid_host_bytes) catch return error.ResourceLimitExceeded;
        return (try session.admitHostPreprocessOwned(amount, owned)).lease;
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
        if (self.admit != null or self.prepaid_host_bytes != 0 or admission.backend_class != .cpu) return error.InvalidAdmissionLeaseReduction;
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
        const reservation = try self.backing.create(Reservation);
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
