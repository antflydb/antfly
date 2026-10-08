// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Process-local execution ownership across independently compiled archives.
//! Tokens are single-worker capsules. I/O, cancellation and clock inputs remain
//! borrowed through finish; parallel children need independent tokens.
const std = @import("std");
const native = @import("runtime_native_abi.zig");
const errors = @import("runtime_error_abi.zig");
const http = @import("runtime_http_abi.zig");

pub const abi_version: u32 = 5;
pub const Class = enum(c_int) { query, write, inference, background, control };
pub const Wait = enum(c_int) { resource, output };

pub const Start = extern struct {
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(@This()),
    io: *const native.IoBorrow,
    clock_io: ?*const native.IoBorrow = null,
    deadline_ns: u64 = 0,
    has_deadline: u8 = 0,
    native_deadline: u8 = 0,
    /// Initial acquisition only; published reentry keeps bounded fair waits.
    allow_wait: u8 = 1,
    /// Trusted transport cleanup, independent of the expired foreground root.
    cleanup: u8 = 0,
    reserved: [4]u8 = @splat(0),
    cancellation: http.CancellationView = .{},
    class: c_int,
    slots: usize = 1,
    retained_bytes: u64 = 0,
    out_handle: *?*anyopaque,

    pub fn validate(self: *const Start) !void {
        if (self.version != abi_version or self.struct_size != @sizeOf(Start)) return error.UnsupportedVersion;
        if (self.has_deadline > 1 or self.native_deadline > 1 or self.allow_wait > 1 or self.cleanup > 1 or !std.mem.allEqual(u8, &self.reserved, 0)) return error.InvalidArgument;
        if (self.native_deadline != 0 and self.clock_io != null) return error.InvalidArgument;
        _ = std.enums.fromInt(Class, self.class) orelse return error.InvalidArgument;
        if (self.slots == 0) return error.InvalidRunnableDemand;
        if (self.cleanup != 0 and (self.class != @backingInt(Class.control) or self.slots != 1 or self.allow_wait != 0 or
            self.has_deadline == 0 or self.cancellation.context != null or self.cancellation.is_cancelled != null)) return error.InvalidArgument;
    }
};

/// Trusted completion executor, borrowed through the owning finish. Its finite
/// deadline is independent of foreground cancellation. Reserve before any
/// potentially durable acceptance; activate only after the owner has arranged
/// task and resource lifetime through completion or authoritative transfer.
pub const Completion = extern struct {
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(@This()),
    io: *const native.IoBorrow,
    clock_io: ?*const native.IoBorrow = null,
    deadline_ns: u64,
    native_deadline: u8 = 0,
    reserved: [7]u8 = @splat(0),

    pub fn validate(self: *const Completion) !void {
        // Inspect the prefix before accessing fields from a foreign layout.
        if (self.version != abi_version or self.struct_size != @sizeOf(Completion)) return error.UnsupportedVersion;
        if (self.native_deadline > 1 or !std.mem.allEqual(u8, &self.reserved, 0)) return error.InvalidArgument;
        if (self.native_deadline != 0 and self.clock_io != null) return error.InvalidArgument;
    }
};

pub const Owner = extern struct {
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(@This()),
    context: *anyopaque,
    retain: *const fn (*anyopaque) callconv(.c) errors.Status,
    release: *const fn (*anyopaque) callconv(.c) void,
    start: *const fn (*anyopaque, *const Start) callconv(.c) errors.Status,
    checkpoint: *const fn (*anyopaque) callconv(.c) errors.Status,
    park: *const fn (*anyopaque, c_int) callconv(.c) errors.Status,
    enter: *const fn (*anyopaque, c_int) callconv(.c) errors.Status,
    finish: *const fn (*anyopaque) callconv(.c) void,
    /// Optional dynamic CPU team capability. All prior children must join
    /// before resizing. Absent support fails explicitly without new admission.
    resize_team: ?*const fn (*anyopaque, usize) callconv(.c) errors.Status = null,
    reserve_completion: ?*const fn (*anyopaque, *const Completion) callconv(.c) errors.Status = null,
    activate_completion: ?*const fn (*anyopaque) callconv(.c) errors.Status = null,

    pub fn reserveCompletion(self: Owner, handle: *anyopaque, options: *const Completion) !void {
        if (!self.valid()) return error.UnsupportedVersion;
        try options.validate();
        const callback = self.reserve_completion orelse return error.UnsupportedOperation;
        const status = callback(handle, options);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn activateCompletion(self: Owner, handle: *anyopaque) !void {
        if (!self.valid()) return error.UnsupportedVersion;
        const callback = self.activate_completion orelse return error.UnsupportedOperation;
        const status = callback(handle);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn resizeTeam(self: Owner, handle: *anyopaque, slots: usize) !void {
        if (!self.valid()) return error.UnsupportedVersion;
        if (slots == 0) return error.InvalidRunnableDemand;
        const resize = self.resize_team orelse return error.UnsupportedOperation;
        const status = resize(handle, slots);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    /// Check the foreign prefix before copying any appended callbacks.
    pub fn read(raw: *const Owner) !Owner {
        if (raw.version != abi_version or raw.struct_size < @sizeOf(Owner)) return error.UnsupportedVersion;
        return raw.*;
    }

    pub fn valid(self: Owner) bool {
        return self.version == abi_version and self.struct_size >= @sizeOf(Owner);
    }
};

/// Stored consumer capability. The caller joins every consumer task before
/// releasing this lease; independently started capsules hold their own leases.
pub const Lease = struct {
    owner: Owner,

    pub fn initFrom(raw: *const Owner) !Lease {
        return init(try Owner.read(raw));
    }

    pub fn init(owner: Owner) !Lease {
        if (!owner.valid()) return error.UnsupportedVersion;
        const status = owner.retain(owner.context);
        if (!status.isOk()) return errors.errorFromStatus(status);
        return .{ .owner = owner };
    }

    pub fn deinit(self: *Lease) void {
        self.owner.release(self.owner.context);
        self.* = undefined;
    }
};

/// A synchronous child borrows the parent's single-worker grant. This value
/// cannot retire or retain its parent; it must not outlive the parent call or
/// be shared by parallel children. cpu_slots is the parent's admitted demand.
pub const Borrowed = extern struct {
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(@This()),
    context: *anyopaque,
    cpu_slots: usize,
    parent_class: c_int,
    checkpoint: *const fn (*anyopaque) callconv(.c) errors.Status,
    park: *const fn (*anyopaque, c_int) callconv(.c) errors.Status,
    /// The parent implementation validates its mandatory resumption fences
    /// before exposing child output to the parent phase.
    enter: *const fn (*anyopaque, c_int) callconv(.c) errors.Status,
    resize_team: ?*const fn (*anyopaque, usize) callconv(.c) errors.Status = null,
    reserve_completion: ?*const fn (*anyopaque, *const Completion) callconv(.c) errors.Status = null,
    activate_completion: ?*const fn (*anyopaque) callconv(.c) errors.Status = null,

    pub fn reserveCompletion(self: Borrowed, options: *const Completion) !void {
        try self.validate();
        try options.validate();
        const callback = self.reserve_completion orelse return error.UnsupportedOperation;
        const status = callback(self.context, options);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn activateCompletion(self: Borrowed) !void {
        try self.validate();
        const callback = self.activate_completion orelse return error.UnsupportedOperation;
        const status = callback(self.context);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    /// Update this snapshot only after the parent has admitted the new team.
    /// Quiesce every prior borrower first; copies do not track later resizing.
    pub fn resizeTeam(self: *Borrowed, slots: usize) !void {
        try self.validate();
        if (slots == 0) return error.InvalidRunnableDemand;
        const resize = self.resize_team orelse return error.UnsupportedOperation;
        const status = resize(self.context, slots);
        if (!status.isOk()) return errors.errorFromStatus(status);
        self.cpu_slots = slots;
    }

    pub fn read(raw: *const Borrowed) !Borrowed {
        if (raw.version != abi_version or raw.struct_size != @sizeOf(Borrowed)) return error.UnsupportedVersion;
        const value = raw.*;
        try value.validate();
        return value;
    }

    pub fn validate(self: Borrowed) !void {
        if (self.version != abi_version or self.struct_size != @sizeOf(Borrowed)) return error.UnsupportedVersion;
        if (self.cpu_slots == 0) return error.InvalidRunnableDemand;
        _ = std.enums.fromInt(Class, self.parent_class) orelse return error.InvalidArgument;
    }

    pub fn fromOwner(owner: Owner, handle: *anyopaque, slots: usize, class: Class) !Borrowed {
        if (!owner.valid()) return error.UnsupportedVersion;
        const borrow: Borrowed = .{
            .context = handle,
            .cpu_slots = slots,
            .parent_class = @backingInt(class),
            .checkpoint = owner.checkpoint,
            .park = owner.park,
            .enter = owner.enter,
            .resize_team = owner.resize_team,
            .reserve_completion = owner.reserve_completion,
            .activate_completion = owner.activate_completion,
        };
        try borrow.validate();
        return borrow;
    }
};

/// Invocation-owned fanout under one retained execution. Demand includes the
/// coordinator plus every concurrently running child. The allocator must be a
/// thread-safe, bounded owner which remains alive through all joined children.
/// This is local trusted state, never a serialized or independently owned root.
pub const Team = struct {
    borrow: Borrowed,
    maximum_slots: usize,
    allocator: std.mem.Allocator,
    state: enum { fresh, active, joined } = .fresh,

    pub fn reserve(self: *Team, child_tasks: usize) !void {
        // A proven aborted attempt may retry after all its children joined.
        // The same parent fences and retained reservation still govern it.
        if (self.state == .active) return error.InvalidRunnableTransition;
        try self.borrow.validate();
        if (self.maximum_slots == 0 or self.borrow.cpu_slots > self.maximum_slots) return error.InvalidRunnableDemand;
        // Never shrink the caller's existing grant during admission. Accepted
        // work retains its admitted team until the execution's owning finish.
        const slots = @max(self.borrow.cpu_slots, @min(self.maximum_slots, child_tasks +| 1));
        try self.borrow.resizeTeam(slots);
        self.state = .active;
    }

    pub fn childCapacity(self: *const Team) usize {
        std.debug.assert(self.state == .active);
        return self.borrow.cpu_slots - 1;
    }

    /// Every child must already be joined. Do not reacquire CPU under the
    /// expired foreground fence or discard an accepted completion obligation.
    /// The owning Execution retains the team grant until its actual finish.
    pub fn finish(self: *Team) void {
        std.debug.assert(self.state == .active);
        self.state = .joined;
    }
};

/// Transfer the existing operation to a synchronous child phase, then return
/// it through the parent's resumption callback. Failure stops the operation;
/// its owning caller still retires it. No fresh admission or deadline is made.
pub const Transfer = struct {
    borrow: Borrowed,
    active: bool = true,

    pub fn initFrom(raw: *const Borrowed, class: Class) !Transfer {
        return init(try Borrowed.read(raw), class);
    }

    pub fn init(borrow: Borrowed, class: Class) !Transfer {
        try borrow.validate();
        try checked(borrow.checkpoint(borrow.context));
        try checked(borrow.park(borrow.context, @backingInt(Wait.resource)));
        try checked(borrow.enter(borrow.context, @backingInt(class)));
        return .{ .borrow = borrow };
    }

    pub fn checkpoint(self: *const Transfer) !void {
        if (!self.active) return error.InvalidRunnableTransition;
        try checked(self.borrow.checkpoint(self.borrow.context));
    }

    pub fn returnToParent(self: *Transfer) !void {
        if (!self.active) return error.InvalidRunnableTransition;
        self.active = false;
        try checked(self.borrow.park(self.borrow.context, @backingInt(Wait.resource)));
        try checked(self.borrow.enter(self.borrow.context, self.borrow.parent_class));
    }

    fn checked(status: errors.Status) !void {
        if (!status.isOk()) return errors.errorFromStatus(status);
    }
};

test "runnable execution ABI rejects malformed inputs before borrowing executor state" {
    var handle: ?*anyopaque = null;
    var start: Start = .{ .io = @ptrFromInt(8), .class = @backingInt(Class.inference), .out_handle = &handle };
    try start.validate();
    start.version = 1;
    try std.testing.expectError(error.UnsupportedVersion, start.validate());
    start.version = abi_version;
    start.allow_wait = 2;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.allow_wait = 0;
    try start.validate();
    start.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, start.validate());
    start.struct_size = @sizeOf(Start);
    start.class = -1;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.class = @backingInt(Class.background);
    start.slots = 0;
    try std.testing.expectError(error.InvalidRunnableDemand, start.validate());
    start.slots = 1;
    start.has_deadline = 2;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.has_deadline = 1;
    start.native_deadline = 1;
    start.clock_io = @ptrFromInt(8);
    try std.testing.expectError(error.InvalidArgument, start.validate());
    try std.testing.expect(handle == null);
}

test "runnable execution ABI retains C layout and validates the owner prefix" {
    try std.testing.expectEqual(.@"extern", @typeInfo(Start).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(Owner).@"struct".layout);
    var owner: Owner = undefined;
    owner.version = abi_version;
    owner.struct_size = @sizeOf(Owner);
    try std.testing.expect(owner.valid());
    owner.struct_size -= 1;
    try std.testing.expect(!owner.valid());
    owner.struct_size = @sizeOf(Owner);
    owner.version += 1;
    try std.testing.expect(!owner.valid());
}

test "runnable execution borrowed ABI validates demand and headers before calling a parent" {
    try std.testing.expectEqual(.@"extern", @typeInfo(Borrowed).@"struct".layout);
    var loan: Borrowed = undefined;
    loan.version = abi_version;
    loan.struct_size = @sizeOf(Borrowed);
    loan.cpu_slots = 1;
    loan.parent_class = @backingInt(Class.query);
    try loan.validate();
    loan.version += 1;
    try std.testing.expectError(error.UnsupportedVersion, loan.validate());
    loan.version = abi_version;
    loan.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, loan.validate());
    loan.struct_size += 1;
    loan.cpu_slots = 0;
    try std.testing.expectError(error.InvalidRunnableDemand, loan.validate());
    loan.cpu_slots = 1;
    loan.parent_class = -1;
    try std.testing.expectError(error.InvalidArgument, loan.validate());
}

test "runnable cleanup ABI requires a bounded independent single-slot control root" {
    var handle: ?*anyopaque = null;
    var start: Start = .{ .io = @ptrFromInt(8), .class = @backingInt(Class.control), .cleanup = 1, .allow_wait = 0, .has_deadline = 1, .deadline_ns = 1, .out_handle = &handle };
    try start.validate();
    start.version = 2;
    try std.testing.expectError(error.UnsupportedVersion, start.validate());
    start.version = abi_version;
    start.cleanup = 2;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.cleanup = 1;
    start.allow_wait = 1;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.allow_wait = 0;
    start.has_deadline = 0;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.has_deadline = 1;
    start.slots = 2;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.slots = 1;
    start.class = @backingInt(Class.query);
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.class = @backingInt(Class.control);
    start.cancellation.context = @ptrFromInt(8);
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.cancellation = .{};
    start.reserved[0] = 1;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    try std.testing.expect(handle == null);
}

test "runnable team ABI rejects legacy and malformed demand before callbacks and preserves failed snapshots" {
    const Fake = struct {
        calls: usize = 0,
        deny: bool = false,
        fn resize(raw: *anyopaque, slots: usize) callconv(.c) errors.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.deny) return errors.statusFromError(error.ResourceTemporarilyUnavailable);
            std.debug.assert(slots > 0);
            return .ok;
        }
        fn check(_: *anyopaque) callconv(.c) errors.Status {
            return .ok;
        }
        fn transition(_: *anyopaque, _: c_int) callconv(.c) errors.Status {
            return .ok;
        }
    };
    var fake: Fake = .{};
    var loan: Borrowed = .{ .context = &fake, .cpu_slots = 1, .parent_class = @backingInt(Class.write), .checkpoint = Fake.check, .park = Fake.transition, .enter = Fake.transition, .resize_team = Fake.resize };
    loan.version = 3;
    try std.testing.expectError(error.UnsupportedVersion, loan.resizeTeam(2));
    loan.version = abi_version;
    loan.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, loan.resizeTeam(2));
    loan.struct_size = @sizeOf(Borrowed);
    try std.testing.expectError(error.InvalidRunnableDemand, loan.resizeTeam(0));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    try loan.resizeTeam(2);
    try std.testing.expectEqual(@as(usize, 2), loan.cpu_slots);
    fake.deny = true;
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, loan.resizeTeam(3));
    try std.testing.expectEqual(@as(usize, 2), loan.cpu_slots);
    loan.resize_team = null;
    try std.testing.expectError(error.UnsupportedOperation, loan.resizeTeam(1));
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
}

test "runnable team ABI foreign readers reject header-only legacy and truncated values" {
    const Header = extern struct { version: u32, struct_size: u32 };
    var owner_header: Header align(@alignOf(Owner)) = .{ .version = 3, .struct_size = @sizeOf(Owner) - @sizeOf(usize) };
    const owner: *const Owner = @ptrCast(&owner_header);
    try std.testing.expectError(error.UnsupportedVersion, Owner.read(owner));
    try std.testing.expectError(error.UnsupportedVersion, Lease.initFrom(owner));
    owner_header.version = abi_version;
    owner_header.struct_size = @sizeOf(Header);
    try std.testing.expectError(error.UnsupportedVersion, Owner.read(owner));
    try std.testing.expectError(error.UnsupportedVersion, Lease.initFrom(owner));
    var borrow_header: Header align(@alignOf(Borrowed)) = .{ .version = 3, .struct_size = @sizeOf(Borrowed) - @sizeOf(usize) };
    const borrow: *const Borrowed = @ptrCast(&borrow_header);
    try std.testing.expectError(error.UnsupportedVersion, Borrowed.read(borrow));
    try std.testing.expectError(error.UnsupportedVersion, Transfer.initFrom(borrow, .inference));
    borrow_header.version = abi_version;
    borrow_header.struct_size = @sizeOf(Header);
    try std.testing.expectError(error.UnsupportedVersion, Borrowed.read(borrow));
    try std.testing.expectError(error.UnsupportedVersion, Transfer.initFrom(borrow, .inference));
}

test "completion ABI rejects old prefixes and malformed options before callbacks" {
    const Fake = struct {
        calls: usize = 0,
        fn check(_: *anyopaque) callconv(.c) errors.Status {
            return .ok;
        }
        fn transition(_: *anyopaque, _: c_int) callconv(.c) errors.Status {
            return .ok;
        }
        fn reserve(raw: *anyopaque, _: *const Completion) callconv(.c) errors.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return errors.statusFromError(error.DeadlineExceeded);
        }
        fn activate(raw: *anyopaque) callconv(.c) errors.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return errors.statusFromError(error.Canceled);
        }
    };
    var fake: Fake = .{};
    var borrowed: Borrowed = .{ .context = &fake, .cpu_slots = 1, .parent_class = @backingInt(Class.write), .checkpoint = Fake.check, .park = Fake.transition, .enter = Fake.transition };
    var options: Completion = .{ .io = @ptrFromInt(8), .deadline_ns = 10 };
    try std.testing.expectError(error.UnsupportedOperation, borrowed.reserveCompletion(&options));
    try std.testing.expectError(error.UnsupportedOperation, borrowed.activateCompletion());
    borrowed.reserve_completion = Fake.reserve;
    borrowed.activate_completion = Fake.activate;
    options.version = 4;
    try std.testing.expectError(error.UnsupportedVersion, borrowed.reserveCompletion(&options));
    options.version = abi_version;
    options.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, borrowed.reserveCompletion(&options));
    options.struct_size = @sizeOf(Completion);
    options.native_deadline = 2;
    try std.testing.expectError(error.InvalidArgument, borrowed.reserveCompletion(&options));
    options.native_deadline = 1;
    options.clock_io = @ptrFromInt(8);
    try std.testing.expectError(error.InvalidArgument, borrowed.reserveCompletion(&options));
    options.clock_io = null;
    options.reserved[0] = 1;
    try std.testing.expectError(error.InvalidArgument, borrowed.reserveCompletion(&options));
    options.reserved = @splat(0);
    borrowed.version = 4;
    try std.testing.expectError(error.UnsupportedVersion, borrowed.reserveCompletion(&options));
    try std.testing.expectError(error.UnsupportedVersion, borrowed.activateCompletion());
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    borrowed.version = abi_version;
    try std.testing.expectError(error.DeadlineExceeded, borrowed.reserveCompletion(&options));
    try std.testing.expectError(error.Canceled, borrowed.activateCompletion());
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    const Prefix = extern struct { version: u32, struct_size: u32 };
    var old: Prefix align(@alignOf(Owner)) = .{ .version = 4, .struct_size = @sizeOf(Prefix) };
    try std.testing.expectError(error.UnsupportedVersion, Owner.read(@ptrCast(&old)));
    try std.testing.expectError(error.UnsupportedVersion, Borrowed.read(@ptrCast(&old)));
    try std.testing.expectError(error.UnsupportedVersion, (@as(*const Completion, @ptrCast(&old))).validate());
}
