// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Stable process-control ABI between the executable and runtime archives.
//! Only borrowed byte views and fixed-width C-layout fields cross this
//! boundary. Each runtime constructs and owns its Zig allocator, I/O runtime,
//! environment map, arena, and argument iterator locally.

pub const abi_version: u32 = 1;

pub const Bytes = extern struct {
    ptr: ?[*]const u8 = null,
    len: usize = 0,

    pub fn init(value: []const u8) Bytes {
        return .{ .ptr = if (value.len == 0) null else value.ptr, .len = value.len };
    }

    pub fn slice(self: Bytes) []const u8 {
        if (self.len == 0) return "";
        return self.ptr.?[0..self.len];
    }

    pub fn valid(self: Bytes) bool {
        return self.len == 0 or self.ptr != null;
    }
};

pub const EnvironmentEntry = extern struct {
    name: Bytes,
    value: Bytes,
};

pub const Context = extern struct {
    version: u32 = abi_version,
    _reserved: u32 = 0,
    command: Bytes,
    arguments_ptr: ?[*]const Bytes = null,
    arguments_len: usize = 0,
    environment_ptr: ?[*]const EnvironmentEntry = null,
    environment_len: usize = 0,

    pub fn arguments(self: *const Context) ?[]const Bytes {
        if (self.arguments_len == 0) return &.{};
        const ptr = self.arguments_ptr orelse return null;
        return ptr[0..self.arguments_len];
    }

    pub fn environment(self: *const Context) ?[]const EnvironmentEntry {
        if (self.environment_len == 0) return &.{};
        const ptr = self.environment_ptr orelse return null;
        return ptr[0..self.environment_len];
    }

    pub fn valid(self: *const Context) bool {
        if (self.version != abi_version or !self.command.valid()) return false;
        const args = self.arguments() orelse return false;
        for (args) |arg| if (!arg.valid()) return false;
        const env = self.environment() orelse return false;
        for (env) |entry| {
            if (!entry.name.valid() or !entry.value.valid() or entry.name.len == 0)
                return false;
        }
        return true;
    }
};

test "runtime process context is C-layout and rejects malformed views" {
    const std = @import("std");
    try std.testing.expectEqual(.@"extern", @typeInfo(Context).@"struct".layout);
    const good: Context = .{ .command = .init("data") };
    try std.testing.expect(good.valid());
    var wrong_version = good;
    wrong_version.version += 1;
    try std.testing.expect(!wrong_version.valid());
    const missing_arguments: Context = .{
        .command = .init("data"),
        .arguments_len = 1,
    };
    try std.testing.expect(!missing_arguments.valid());
}
