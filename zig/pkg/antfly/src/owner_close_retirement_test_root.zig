// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

pub const antfly_sources = @import("source_owner_control.zig");
pub const consumer_tests_only = true;
pub const owner_close_only_test = true;
// The fixture exercises the actual Source registry/close protocol, with no DB
// behind the handle. Physical storage close is deliberately outside its scope.
fn close(_: ?*anyopaque) callconv(.c) void {}
const abi = @import("kernel_owner_abi");
pub var drain_status: abi.Status = .ok;
pub var drain_calls: usize = 0;
fn drain(_: ?*anyopaque, request: *const abi.CloseDrainRequest) callconv(.c) abi.Status {
    if (request.version != abi.abi_version) return .invalid_abi;
    if (request.deadline_ns == 0 or request.execution_runnable == null or request.clock_io == null) return .invalid_argument;
    drain_calls += 1;
    return drain_status;
}
comptime {
    @export(&drain, .{ .name = "antfly_storage_owner_drain_close" });
    @export(&close, .{ .name = "antfly_storage_owner_close" });
}
test {
    _ = @import("api/kernel_owner_source.zig");
    _ = @import("api/runnable_owner_execution.zig");
}
