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

//! Consumer sees tensor descriptors and status identities, never kernels.
const std = @import("std");
const abi = @import("architecture_probe_abi");
const identity = @import("runtime_failure_identity");

fn run(request: abi.Tensor) !void {
    var failure: abi.failure.FailureIdentity = .{};
    const status = abi.antfly_probe_fused_cpu(&request, &failure);
    try identity.validateFailureEnvelope(status, &failure, abi.version);
    try identity.statusToError(status);
}

test "compiled fused CPU owner rotates complete tensors and applies inverse" {
    const x = [_]f32{ 1, 2, 3, 4 };
    const cos = [_]f32{ 0, 1 };
    const sin = [_]f32{ 1, 0 };
    var out: [4]f32 = undefined;
    var restored: [4]f32 = undefined;
    var request: abi.Tensor = .{ .operation = .rope, .input = .{ .ptr = &x, .len = x.len }, .output = .{ .ptr = &out, .len = out.len }, .cos = .{ .ptr = &cos, .len = cos.len }, .sin = .{ .ptr = &sin, .len = sin.len }, .width = 4, .head_dim = 4 };
    try run(request);
    try std.testing.expectEqualSlices(f32, &.{ -3, 2, 1, 4 }, &out);
    request.input.ptr = &out;
    request.output.ptr = &restored;
    request.inverse = 1;
    try run(request);
    try std.testing.expectEqualSlices(f32, &x, &restored);
}

test "compiled fused CPU owner retains GEGLU forward and backward math" {
    const x = [_]f32{ 0, 12, 2, 3 };
    var out: [2]f32 = undefined;
    try run(.{ .operation = .geglu, .width = 2, .input = .{ .ptr = &x, .len = x.len }, .output = .{ .ptr = &out, .len = out.len } });
    try std.testing.expectEqualSlices(f32, &.{ 0, 36 }, &out);
    const gradient = [_]f32{ 1, 1 };
    var dx: [4]f32 = undefined;
    try run(.{ .operation = .geglu_backward, .width = 2, .input = .{ .ptr = &x, .len = x.len }, .output = .{ .ptr = &dx, .len = dx.len }, .gradient = .{ .ptr = &gradient, .len = gradient.len } });
    try std.testing.expectEqualSlices(f32, &.{ 1, 3, 0, 12 }, &dx);
}

test "compiled fused CPU owner rejects malformed descriptors and dimensions" {
    var out: [4]f32 = @splat(42);
    const base: abi.Tensor = .{ .operation = .rope, .width = 4, .head_dim = 4, .input = .{ .len = 4 }, .output = .{ .ptr = &out, .len = out.len } };
    try std.testing.expectError(error.InvalidArgument, run(base));
    var bad = base;
    bad.abi_version = abi.version + 1;
    try std.testing.expectError(error.InvalidAbiVersion, run(bad));
    bad = base;
    bad.input = .{ .ptr = &out, .len = out.len };
    try std.testing.expectError(error.InvalidRopeTable, run(bad));
    bad.width = 0;
    bad.operation = .qkv_rope;
    try std.testing.expectError(error.InvalidArgument, run(bad));
    try std.testing.expectEqualSlices(f32, &.{ 42, 42, 42, 42 }, &out);
}
