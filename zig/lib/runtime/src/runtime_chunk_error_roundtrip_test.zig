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

//! Compile this provider and its consumer independently: Zig error integers
//! must differ while the checked callback preserves model-chunk failures.
const std = @import("std");
const builtin = @import("builtin");
const callback = @import("runtime_callback_abi.zig");
const Methods = struct { fail: *const fn (u8) anyerror!u64 };
const Boundary = callback.Boundary(Methods);
const Descriptor = extern struct {
    dispatch: callback.CallbackDispatch,
    fail: *const anyopaque,
    raw_code: *const fn (u8) callconv(.c) u16,
};
fn fail(index: u8) anyerror!u64 {
    return switch (index) {
        0 => error.StaleChunkContinuation,
        1 => error.IncompleteChunkCoverage,
        2 => error.SourceVersionChanged,
        3 => error.MemoryBudgetExceeded,
        4 => error.UnsupportedChunkMediaType,
        5 => error.EmbeddingIdentityMismatch,
        6 => error.InvalidChunkPolicy,
        7 => error.InvalidChunkReplay,
        8 => error.DocumentTokenLimitExceeded,
        else => 42,
    };
}
fn rawCode(index: u8) callconv(.c) u16 {
    _ = fail(index) catch |err| return @intFromError(err);
    return 0;
}
fn descriptor() callconv(.c) *const Descriptor {
    const Holder = struct {
        const value: Descriptor = .{ .dispatch = Boundary.local_dispatch, .fail = @ptrCast(&fail), .raw_code = rawCode };
    };
    return &Holder.value;
}
comptime {
    if (!builtin.is_test) @export(&descriptor, .{ .name = "antfly_test_chunk_error_provider_v1", .visibility = .hidden });
}
extern fn antfly_test_chunk_error_provider_v1() callconv(.c) *const Descriptor;

test "model-aware chunk failures cross independent native error domains" {
    const provider = antfly_test_chunk_error_provider_v1();
    const method: @FieldType(Methods, "fail") = @ptrCast(@alignCast(provider.fail));
    var distinct = false;
    for ([_][]const u8{
        "StaleChunkContinuation",     "IncompleteChunkCoverage",   "SourceVersionChanged", "MemoryBudgetExceeded",
        "UnsupportedChunkMediaType",  "EmbeddingIdentityMismatch", "InvalidChunkPolicy",   "InvalidChunkReplay",
        "DocumentTokenLimitExceeded",
    }, 0..) |name, index| {
        const expected = @import("storage_engine_errors").fromName(name);
        const code: u8 = @intCast(index);
        distinct = distinct or provider.raw_code(code) != @intFromError(expected);
        try std.testing.expectError(expected, Boundary.call("fail", provider.dispatch, method, .{code}));
    }
    try std.testing.expect(distinct);
    try std.testing.expectEqual(@as(u64, 42), try Boundary.call("fail", provider.dispatch, method, .{@as(u8, 255)}));
}
