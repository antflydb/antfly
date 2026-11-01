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

//! Independent compilation of existing fused CPU tensor operations. One call
//! processes a complete tensor; numerical inner loops stay with this owner.
const std = @import("std");
const abi = @import("architecture_probe_abi");
const identity = @import("runtime_failure_identity");
const rope = @import("ops/rope_table.zig");
const geglu = @import("ops/geglu.zig");

fn input(value: abi.Input) ![]const f32 {
    if (value.len > std.math.maxInt(usize) / @sizeOf(f32) or (value.len != 0 and value.ptr == null)) return error.InvalidArgument;
    return if (value.len == 0) &.{} else value.ptr.?[0..@intCast(value.len)];
}

fn execute(request: *const abi.Tensor) !void {
    if (request.abi_version != abi.version) return error.InvalidAbiVersion;
    if (request.output.len > std.math.maxInt(usize) / @sizeOf(f32) or (request.output.len != 0 and request.output.ptr == null)) return error.InvalidArgument;
    const out: []f32 = if (request.output.len == 0) &.{} else request.output.ptr.?[0..@intCast(request.output.len)];
    const x = try input(request.input);
    const width = std.math.cast(usize, request.width) orelse return error.InvalidArgument;
    const head_dim = std.math.cast(usize, request.head_dim) orelse return error.InvalidArgument;
    if (width == 0 or request.inverse > 1) return error.InvalidArgument;
    // Check multiplication before calling the existing kernels, whose
    // preconditions assume dimensions came from a validated tensor shape.
    _ = std.math.mul(usize, width, 3) catch return error.InvalidArgument;
    switch (request.operation) {
        .rope => try rope.apply(out, x, try input(request.cos), try input(request.sin), width, head_dim, request.inverse != 0),
        .qkv_rope => try rope.applyQkv(out, x, try input(request.cos), try input(request.sin), width, head_dim, request.inverse != 0),
        .geglu => try geglu.forward(out, x, width),
        .geglu_backward => try geglu.backward(out, x, try input(request.gradient), width),
        _ => return error.InvalidArgument,
    }
}

pub export fn antfly_probe_fused_cpu(request: *const abi.Tensor, failure: *abi.failure.FailureIdentity) callconv(.c) abi.failure.Status {
    failure.* = .{};
    execute(request) catch |err| {
        failure.* = identity.failureFromError(err, .inference_runtime, abi.version, 1);
        return failure.status;
    };
    return .ok;
}
