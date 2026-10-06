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
