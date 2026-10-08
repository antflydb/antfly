// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Process-local execution ownership across independently compiled archives.
//! Tokens are single-worker capsules. I/O, cancellation and clock inputs remain
//! borrowed through finish; parallel children need independent tokens.
const std = @import("std");
const native = @import("runtime_native_abi.zig");
const errors = @import("runtime_error_abi.zig");
const http = @import("runtime_http_abi.zig");

pub const abi_version: u32 = 2;
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
    reserved: [5]u8 = @splat(0),
    cancellation: http.CancellationView = .{},
    class: c_int,
    slots: usize = 1,
    retained_bytes: u64 = 0,
    out_handle: *?*anyopaque,

    pub fn validate(self: *const Start) !void {
        if (self.version != abi_version or self.struct_size != @sizeOf(Start)) return error.UnsupportedVersion;
        if (self.has_deadline > 1 or self.native_deadline > 1 or self.allow_wait > 1 or !std.mem.allEqual(u8, &self.reserved, 0)) return error.InvalidArgument;
        if (self.native_deadline != 0 and self.clock_io != null) return error.InvalidArgument;
        _ = std.enums.fromInt(Class, self.class) orelse return error.InvalidArgument;
        if (self.slots == 0) return error.InvalidRunnableDemand;
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

    pub fn valid(self: Owner) bool {
        return self.version == abi_version and self.struct_size >= @sizeOf(Owner);
    }
};

/// Stored consumer capability. The caller joins every consumer task before
/// releasing this lease; independently started capsules hold their own leases.
pub const Lease = struct {
    owner: Owner,

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
        };
        try borrow.validate();
        return borrow;
    }
};

/// Transfer the existing operation to a synchronous child phase, then return
/// it through the parent's resumption callback. Failure stops the operation;
/// its owning caller still retires it. No fresh admission or deadline is made.
pub const Transfer = struct {
    borrow: Borrowed,
    active: bool = true,

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
