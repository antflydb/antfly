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

//! Storage-free dispatcher for complete owner-issued query operations.
const provider = @import("storage/local_query_dispatch.zig");
comptime {
    @export(&provider.execute, .{ .name = "antfly_local_query_execute_v2", .visibility = .hidden });
    @export(&provider.bufferDestroy, .{ .name = "antfly_local_query_buffer_destroy", .visibility = .hidden });
}

const std = @import("std");
const abi = @import("local_query_abi");
const identity = @import("kernel_error_identity");

test "opaque query dispatcher rejects stale capabilities before invoking poisoned state" {
    const Call = struct {
        fn run(_: ?*anyopaque, _: *const abi.LocalQueryRequest, _: *abi.QueryOwnedResponse, _: *abi.FailureIdentity) callconv(.c) abi.Status {
            @panic("stale capability invoked");
        }
    };
    var request: abi.LocalQueryRequest = .{ .execution = .{ .context = @ptrFromInt(1), .execute_fn = Call.run } };
    var response: abi.QueryOwnedResponse = .{};
    var failure: abi.FailureIdentity = .{};
    request.execution.version = 0;
    try std.testing.expectEqual(abi.Status.invalid_abi, provider.execute(&request, &response, &failure));
    try std.testing.expectEqualStrings("InvalidAbiVersion", failure.errorName());
    try std.testing.expectEqual(@as(u64, 0), response.buffer.len);
    request.execution.version = abi.execution_abi_version;
    request.execution._reserved = 1;
    try std.testing.expectEqual(abi.Status.invalid_abi, provider.execute(&request, &response, &failure));
    request.execution = .{};
    try std.testing.expectEqual(abi.Status.invalid_argument, provider.execute(&request, &response, &failure));
}

test "opaque query dispatcher forwards one whole operation and owns response destruction" {
    const Owner = struct {
        calls: usize = 0,
        fn run(context: ?*anyopaque, request: *const abi.LocalQueryRequest, response: *abi.QueryOwnedResponse, failure: *abi.FailureIdentity) callconv(.c) abi.Status {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            self.calls += 1;
            std.debug.assert(request.kind == .search);
            std.debug.assert(std.mem.eql(u8, request.table_name.slice(), "docs"));
            std.debug.assert(request.execution_deadline_ns == 123);
            const bytes = std.heap.c_allocator.dupe(u8, "{\"responses\":[]}") catch {
                failure.* = identity.failureFromError(error.OutOfMemory, .local_query, abi.abi_version, @backingInt(abi.LocalQueryOperation.encode_internal_response));
                return failure.status;
            };
            response.* = .{ .buffer = .{ .ptr = bytes.ptr, .len = bytes.len }, .identity_read_generation = 77, .has_identity_read_generation = 1 };
            return .ok;
        }
    };
    var owner: Owner = .{};
    var request: abi.LocalQueryRequest = .{ .execution = .{ .context = &owner, .execute_fn = Owner.run }, .table_name = .fromSlice("docs"), .request_json = .fromSlice("{}"), .has_execution_deadline = 1, .execution_deadline_ns = 123 };
    var response: abi.QueryOwnedResponse = .{};
    defer provider.bufferDestroy(&response.buffer);
    var failure: abi.FailureIdentity = .{};
    try std.testing.expectEqual(abi.Status.ok, provider.execute(&request, &response, &failure));
    try std.testing.expectEqual(@as(usize, 1), owner.calls);
    try std.testing.expectEqualStrings("{\"responses\":[]}", response.buffer.slice());
    try std.testing.expectEqual(@as(u64, 77), response.identity_read_generation);
    try std.testing.expectEqual(@as(u8, 1), response.has_identity_read_generation);
    provider.bufferDestroy(&response.buffer);
    try std.testing.expectEqual(@as(u64, 0), response.buffer.len);
}

test "opaque query dispatcher preserves owner failure stage and identity" {
    const Owner = struct {
        fn run(_: ?*anyopaque, _: *const abi.LocalQueryRequest, _: *abi.QueryOwnedResponse, failure: *abi.FailureIdentity) callconv(.c) abi.Status {
            failure.* = identity.failureFromError(error.DeadlineExceeded, .local_query, abi.abi_version, @backingInt(abi.LocalQueryOperation.execute_internal_query));
            return failure.status;
        }
    };
    var context: u8 = 0;
    const request: abi.LocalQueryRequest = .{ .execution = .{ .context = &context, .execute_fn = Owner.run } };
    var response: abi.QueryOwnedResponse = .{};
    var failure: abi.FailureIdentity = .{};
    const status = provider.execute(&request, &response, &failure);
    try identity.validateFailureEnvelope(status, &failure, abi.abi_version);
    try std.testing.expectEqual(abi.Status.deadline_exceeded, status);
    try std.testing.expectEqualStrings("DeadlineExceeded", failure.errorName());
    try std.testing.expectEqual(@backingInt(abi.LocalQueryOperation.execute_internal_query), failure.operation);
}
