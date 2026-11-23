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

//! Owned ICU provider authority across independently compiled runtimes.
//! This port conveys handles and C-layout values only. No Zig allocator,
//! error union, provider layout or borrowed library path crosses the boundary.
const std = @import("std");
const errors = @import("runtime_error_abi.zig");
pub const abi_version: u32 = 1;
fn checkStatus(result: errors.Status) !void {
    if (result.code != 0) return errors.errorFromStatus(result);
    if (result.detail != 0) return error.SqlIcuProviderAbiMismatch;
}
pub const Version = extern struct {
    collation: [4]u8,
    uca: [4]u8,
    pub fn eql(self: Version, other: Version) bool {
        return std.meta.eql(self, other);
    }
};
pub const VTable = extern struct {
    abi_version: u32 = abi_version,
    struct_size: u32 = @sizeOf(VTable),
    retain: *const fn (*anyopaque) callconv(.c) void,
    release: *const fn (*anyopaque) callconv(.c) void,
    prepare: *const fn (*anyopaque, [*]const u8, usize, u8, ?*const Version, *?*anyopaque, *Version) callconv(.c) errors.Status,
    compare: *const fn (*anyopaque, [*]const u8, usize, [*]const u8, usize, *i8) callconv(.c) errors.Status,
    weights: *const fn (*anyopaque, [*]const u16, usize, [*]u8, u32, *u32) callconv(.c) errors.Status,
    close: *const fn (*anyopaque) callconv(.c) void,
};
pub const Borrow = extern struct {
    abi_version: u32 = abi_version,
    struct_size: u32 = @sizeOf(Borrow),
    context: *anyopaque,
    vtable: *const VTable,
    /// Acquire an owned receiver while the producer's startup owner is alive.
    pub fn receive(self: Borrow) !Receiver {
        if (self.abi_version != abi_version or self.struct_size != @sizeOf(Borrow) or
            self.vtable.abi_version != abi_version or self.vtable.struct_size != @sizeOf(VTable))
            return error.SqlIcuProviderAbiMismatch;
        self.vtable.retain(self.context);
        return .{ .port = self };
    }
};
pub const Receiver = struct {
    port: Borrow,
    pub fn clone(self: Receiver) Receiver {
        self.port.vtable.retain(self.port.context);
        return self;
    }
    pub fn deinit(self: *Receiver) void {
        self.port.vtable.release(self.port.context);
        self.* = undefined;
    }
    pub fn prepare(self: Receiver, locale: []const u8, deterministic: bool, expected: ?Version) !Collator {
        var handle: ?*anyopaque = null;
        var version: Version = undefined;
        const expected_ptr: ?*const Version = if (expected) |*value| value else null;
        const result = self.port.vtable.prepare(self.port.context, locale.ptr, locale.len, @intFromBool(deterministic), expected_ptr, &handle, &version);
        try checkStatus(result);
        const prepared_handle = handle orelse return error.SqlIcuProviderAbiMismatch;
        if (expected) |required| if (!version.eql(required)) {
            self.port.vtable.close(prepared_handle);
            return error.SqlCollationVersionMismatch;
        };
        return .{ .handle = prepared_handle, .vtable = self.port.vtable, .version = version };
    }
};
/// Its provider holds a lease independently of the factory Receiver. Every
/// prepared statement/schema can therefore outlive its creating runtime owner.
pub const Collator = struct {
    handle: *anyopaque,
    vtable: *const VTable,
    version: Version,
    pub fn deinit(self: *Collator) void {
        self.vtable.close(self.handle);
        self.* = undefined;
    }
    pub fn compare(self: Collator, lhs: []const u8, rhs: []const u8) !std.math.Order {
        var order: i8 = 0;
        const result = self.vtable.compare(self.handle, lhs.ptr, lhs.len, rhs.ptr, rhs.len, &order);
        try checkStatus(result);
        return switch (order) {
            -1 => .lt,
            0 => .eq,
            1 => .gt,
            else => error.SqlIcuProviderAbiMismatch,
        };
    }
    pub fn weights(self: Collator, text: []const u16, output: []u8) !u32 {
        const capacity = std.math.cast(u32, output.len) orelse return error.SqlCollationKeyTooLarge;
        var required: u32 = 0;
        const result = self.vtable.weights(self.handle, text.ptr, text.len, output.ptr, capacity, &required);
        try checkStatus(result);
        return required;
    }
};

/// Runtime-only authority. A configured but absent factory cannot fall back to
/// the consumer process's default provider. Schemas acquire an independent
/// lease before retaining this borrowed factory beyond their creating call.
pub const Context = struct {
    configured: bool = false,
    factory: ?Borrow = null,
    pub fn fromBorrow(factory: ?Borrow) Context {
        return .{ .configured = true, .factory = factory };
    }
    pub fn acquire(self: Context) !?Receiver {
        return if (self.factory) |factory| try factory.receive() else null;
    }
};
