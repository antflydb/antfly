// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Stable host callback used by the API kernel for the runtime-reserved local
//! inference connection. The callback is synchronous: request views and the
//! response sink are borrowed only for the call, while response bytes are
//! allocated with the caller-provided ABI allocator and become caller-owned.

const std = @import("std");
const error_abi = @import("runtime_error_abi.zig");
const http_abi = @import("runtime_http_abi.zig");
const memory_abi = @import("runtime_memory_abi.zig");

pub const abi_version: u32 = 1;
pub const Status = error_abi.Status;
pub const statusFromError = error_abi.statusFromError;
pub const errorFromStatus = error_abi.errorFromStatus;
pub const Bytes = http_abi.Bytes;
pub const CancellationView = http_abi.CancellationView;
pub const StreamSink = http_abi.StreamSink;
pub const Allocator = memory_abi.Allocator;
pub const OwnedBytes = memory_abi.OwnedBytes;
pub const OptionalOwnedBytes = memory_abi.OptionalOwnedBytes;

pub const Capability = struct {
    pub const streaming_response: u64 = 1 << 0;
};

pub const InvokeResponse = extern struct {
    status: u16 = 500,
    _reserved: [6]u8 = @splat(0),
    body: OwnedBytes = .{},
    retry_after: OptionalOwnedBytes = .{},
    content_type: OptionalOwnedBytes = .{},

    pub fn valid(self: InvokeResponse) bool {
        return std.mem.allEqual(u8, &self._reserved, 0) and
            validOwnedBytes(self.body) and
            validOptionalOwnedBytes(self.retry_after) and
            validOptionalOwnedBytes(self.content_type);
    }

    /// Releases any caller-owned fields, including a partially populated
    /// response returned alongside an error status.
    pub fn deinit(self: *InvokeResponse, allocator: *const Allocator) void {
        releaseOwnedBytes(allocator, self.body);
        releaseOwnedBytes(allocator, self.retry_after.bytes);
        releaseOwnedBytes(allocator, self.content_type.bytes);
        self.* = .{};
    }
};

fn validOwnedBytes(bytes: OwnedBytes) bool {
    return bytes.len == 0 or bytes.ptr != null;
}

fn validOptionalOwnedBytes(bytes: OptionalOwnedBytes) bool {
    if (bytes.present > 1) return false;
    if (bytes.present == 0) return bytes.bytes.ptr == null and bytes.bytes.len == 0;
    return validOwnedBytes(bytes.bytes);
}

fn releaseOwnedBytes(allocator: *const Allocator, bytes: OwnedBytes) void {
    if (bytes.len != 0) allocator.release(allocator.context, bytes.ptr, bytes.len, @alignOf(u8));
}

pub const InvokeContext = extern struct {
    abi_version: u32,
    struct_size: u32 = @sizeOf(@This()),
    target_context: *anyopaque,
    allocator: *const Allocator,
    operation: Bytes,
    body: Bytes,
    cancellation: CancellationView = .{},
    /// Absolute process-monotonic deadline. Zero means no deadline.
    deadline_ns: u64 = 0,
    stream: StreamSink = .{},
    out_response: *InvokeResponse,
};

pub const Target = extern struct {
    abi_version: u32 = abi_version_value,
    struct_size: u32 = @sizeOf(@This()),
    capabilities: u64,
    context: *anyopaque,
    invoke: *const fn (*const InvokeContext) callconv(.c) Status,

    const abi_version_value = abi_version;

    pub fn valid(self: Target, required_capabilities: u64) bool {
        const known_capabilities = Capability.streaming_response;
        return self.abi_version == abi_version and
            self.struct_size >= @sizeOf(Target) and
            required_capabilities & ~known_capabilities == 0 and
            self.capabilities & required_capabilities == required_capabilities;
    }
};

pub fn validInvokeContext(context: *const InvokeContext) bool {
    const stream_callbacks = @as(u2, @intFromBool(context.stream.start != null)) +
        @as(u2, @intFromBool(context.stream.write != null)) +
        @as(u2, @intFromBool(context.stream.close != null));
    return context.abi_version == abi_version and
        context.struct_size >= @sizeOf(InvokeContext) and
        context.allocator.valid() and
        (stream_callbacks == 0 or stream_callbacks == 3);
}

test "local inference connection ABI retains C layout and validates capabilities" {
    try std.testing.expectEqual(.@"extern", @typeInfo(InvokeResponse).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(InvokeContext).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(Target).@"struct".layout);

    const Callback = struct {
        fn invoke(_: *const InvokeContext) callconv(.c) Status {
            return .ok;
        }
    };
    var byte: u8 = 0;
    const target: Target = .{
        .capabilities = Capability.streaming_response,
        .context = &byte,
        .invoke = Callback.invoke,
    };
    try std.testing.expect(target.valid(Capability.streaming_response));
    try std.testing.expect(!target.valid(1 << 63));
}
