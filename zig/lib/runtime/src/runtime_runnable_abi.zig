// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Process-local execution ownership across independently compiled archives.
//! Tokens are single-worker capsules. I/O, cancellation and clock inputs remain
//! borrowed through finish; parallel children need independent tokens.
const std = @import("std");
const native = @import("runtime_native_abi.zig");
const errors = @import("runtime_error_abi.zig");
const http = @import("runtime_http_abi.zig");

pub const abi_version: u32 = 1;
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
    reserved: [6]u8 = @splat(0),
    cancellation: http.CancellationView = .{},
    class: c_int,
    slots: usize = 1,
    retained_bytes: u64 = 0,
    out_handle: *?*anyopaque,

    pub fn validate(self: *const Start) !void {
        if (self.version != abi_version or self.struct_size != @sizeOf(Start)) return error.UnsupportedVersion;
        if (self.has_deadline > 1 or self.native_deadline > 1 or !std.mem.allEqual(u8, &self.reserved, 0)) return error.InvalidArgument;
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

test "runnable execution ABI rejects malformed inputs before borrowing executor state" {
    var handle: ?*anyopaque = null;
    var start: Start = .{ .io = @ptrFromInt(8), .class = @backingInt(Class.inference), .out_handle = &handle };
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
