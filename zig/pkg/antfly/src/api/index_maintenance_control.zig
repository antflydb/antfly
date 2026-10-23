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

//! Shared control-plane lifetime for graph metric and relational maintenance.
//! Resource adapters retain their own generation proofs and durable executors.
const operation = @import("antfly_local_sources").api_operation;

pub const Control = struct {
    request: operation.RequestContext,

    pub fn cancellation(self: *const Control) operation.CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = cancelled, .check_fn = check };
    }

    fn cancelled(ptr: *const anyopaque) bool {
        check(ptr) catch return true;
        return false;
    }

    fn check(ptr: *const anyopaque) !void {
        const self: *const Control = @ptrCast(@alignCast(ptr));
        try self.request.ensureActive();
    }
};

test "index maintenance shared control preserves deadline and cancellation" {
    const std = @import("std");
    const control = Control{ .request = .{ .deadline_ns = 0 } };
    try std.testing.expect(control.cancellation().isCancelled());
    try std.testing.expectError(error.DeadlineExceeded, control.cancellation().check());
}
