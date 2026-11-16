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

//! Standalone regression harness: native failures are expected results, so
//! count their diagnostics explicitly rather than treating each as a failed
//! Zig unit test. No production error logging or behavior is suppressed.
const std = @import("std");
const pjrt = @import("pjrt.zig");
const c = @import("pjrt_c_types.zig");

var logged_errors: usize = 0;
pub const std_options: std.Options = .{ .logFn = log };
fn log(comptime level: std.log.Level, comptime scope: @EnumLiteral(), comptime format: []const u8, args: anytype) void {
    if (level == .err) {
        logged_errors += 1;
    } else std.log.defaultLog(level, scope, format, args);
}

const Mode = enum { success, no_event, completion_failure, call_failure, missing_buffer };
const Fake = struct {
    var mode: Mode = .success;
    var buffers_created: usize = 0;
    var buffers_destroyed: usize = 0;
    var events_created: usize = 0;
    var events_destroyed: usize = 0;
    var events_awaited: usize = 0;
    var errors_destroyed: usize = 0;
    var arguments_valid: bool = true;
    var expected_kind: c.BufferType = .float32;
    var expected_device: *c.PjrtDevice = undefined;

    fn upload(args: *c.ClientBufferFromHostBufferArgs) callconv(.c) ?*c.PjrtError {
        arguments_valid = args.struct_size == @sizeOf(@TypeOf(args.*)) and
            args.buffer_type == expected_kind and args.device == expected_device and
            args.data != null and args.num_dims == 1 and args.dims != null and args.dims.?[0] == 1 and
            args.host_buffer_semantics == .immutable_only_during_call and args.memory == null and args.device_layout == null;
        // An API error does not transfer any output ownership to the caller.
        if (mode == .call_failure) return @ptrFromInt(3);
        if (mode != .missing_buffer) {
            buffers_created += 1;
            args.buffer = @ptrFromInt(1);
        }
        if (mode != .no_event) {
            events_created += 1;
            args.done_with_host_buffer = @ptrFromInt(2);
        }
        return null;
    }
    fn destroyBuffer(_: *c.BufferDestroyArgs) callconv(.c) ?*c.PjrtError {
        buffers_destroyed += 1;
        return null;
    }
    fn awaitEvent(_: *c.EventAwaitArgs) callconv(.c) ?*c.PjrtError {
        events_awaited += 1;
        return if (mode == .completion_failure) @ptrFromInt(3) else null;
    }
    fn destroyEvent(_: *c.EventDestroyArgs) callconv(.c) ?*c.PjrtError {
        events_destroyed += 1;
        return null;
    }
    fn message(args: *c.ErrorMessageArgs) callconv(.c) void {
        const text = "expected upload failure";
        args.message = text;
        args.message_size = text.len;
    }
    fn destroyError(_: *c.ErrorDestroyArgs) callconv(.c) void {
        errors_destroyed += 1;
    }
};

pub fn main() !void {
    var api = std.mem.zeroes(c.Api);
    api.struct_size = @sizeOf(c.Api);
    api._fns[c.Api.FN_CLIENT_BUFFER_FROM_HOST] = @ptrCast(&Fake.upload);
    api._fns[c.Api.FN_BUFFER_DESTROY] = @ptrCast(&Fake.destroyBuffer);
    api._fns[c.Api.FN_EVENT_AWAIT] = @ptrCast(&Fake.awaitEvent);
    api._fns[c.Api.FN_EVENT_DESTROY] = @ptrCast(&Fake.destroyEvent);
    api._fns[c.Api.FN_ERROR_MESSAGE] = @ptrCast(&Fake.message);
    api._fns[c.Api.FN_ERROR_DESTROY] = @ptrCast(&Fake.destroyError);
    const client = pjrt.Client{ .lib = undefined, .api = &api, .handle = @ptrFromInt(1), .device = @ptrFromInt(1) };
    inline for (std.meta.tags(Mode)) |mode| {
        for (0..3) |variant| {
            Fake.mode = mode;
            Fake.buffers_created = 0;
            Fake.buffers_destroyed = 0;
            Fake.events_created = 0;
            Fake.events_destroyed = 0;
            Fake.events_awaited = 0;
            Fake.errors_destroyed = 0;
            logged_errors = 0;
            Fake.expected_kind = if (variant == 1) .int64 else .float32;
            Fake.expected_device = @ptrFromInt(if (variant == 2) @as(usize, 2) else 1);
            const result = switch (variant) {
                0 => client.bufferFromHostFloat32(&.{7}, &.{1}),
                1 => client.bufferFromHostInt64(&.{7}, &.{1}),
                else => client.bufferFromHostFloat32ForDevice(&.{7}, &.{1}, Fake.expected_device),
            };
            if (mode == .success or mode == .no_event) {
                var buffer = try result;
                try std.testing.expectEqual(@as(usize, 0), Fake.buffers_destroyed);
                buffer.deinit();
            } else try std.testing.expectError(error.PjrtApiError, result);
            try std.testing.expect(Fake.arguments_valid);
            try std.testing.expectEqual(Fake.buffers_created, Fake.buffers_destroyed);
            try std.testing.expectEqual(Fake.events_created, Fake.events_destroyed);
            try std.testing.expectEqual(@as(usize, if (mode == .call_failure or mode == .no_event) 0 else 1), Fake.events_awaited);
            const native_errors: usize = if (mode == .call_failure or mode == .completion_failure) 1 else 0;
            try std.testing.expectEqual(native_errors, Fake.errors_destroyed);
            try std.testing.expectEqual(native_errors, logged_errors);
        }
    }
}
