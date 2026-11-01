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

//! Root exact production index lifecycle operations as an independent archive.
//! This probe does not transfer physical-store or transaction ownership.
const std = @import("std");
const abi = @import("architecture_probe_abi");
const identity = @import("runtime_failure_identity");
const Index = @import("antfly_local_sources").storage_db_algebraic_index.Index;
pub const antfly_sources = @import("source_owner_storage.zig");

fn bytes(value: abi.Bytes) ![]const u8 {
    if (value.len > std.math.maxInt(usize) or (value.len != 0 and value.ptr == null)) return error.InvalidArgument;
    return if (value.len == 0) "" else value.ptr.?[0..@intCast(value.len)];
}

pub export fn antfly_probe_index_open(request: *const abi.IndexOpen, output: *?*anyopaque, failure: *abi.failure.FailureIdentity) callconv(.c) abi.failure.Status {
    output.* = null;
    failure.* = .{};
    const index = open(request) catch |err| {
        failure.* = identity.failureFromError(err, .storage_owner, abi.version, 1);
        return failure.status;
    };
    output.* = index;
    return .ok;
}

fn open(request: *const abi.IndexOpen) !*Index {
    if (request.abi_version != abi.version) return error.InvalidAbiVersion;
    return Index.create(std.heap.c_allocator, try bytes(request.name), try bytes(request.config));
}

pub export fn antfly_probe_index_close(handle: ?*anyopaque) callconv(.c) void {
    const index: *Index = @ptrCast(@alignCast(handle orelse return));
    index.destroy();
}
